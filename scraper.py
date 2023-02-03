#
# Periodically scrape the layer maps of one or more timelines
# and store the results in an SQL database.
#

import argparse
import asyncio
import json
import logging
from os import getenv
import sys
from typing import Any, Dict, List, Optional, Set, Tuple
import datetime

import aiohttp
import asyncpg


class ClientException(Exception):
    pass


class Client:
    def __init__(self, pageserver_api_endpoint: str):
        self.endpoint = pageserver_api_endpoint
        self.sess = aiohttp.ClientSession()

    async def close(self):
        await self.sess.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        await self.close()

    async def get_pageserver_id(self):
        resp = await self.sess.get(f"{self.endpoint}/v1/status")
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, dict):
            raise ClientException("expecting dict")
        return body["id"]

    async def get_tenant_ids(self):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant")
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, list):
            raise ClientException("expecting list")
        return [t["id"] for t in body]

    async def get_timeline_ids(self, tenant_id):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant/{tenant_id}/timeline")
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, list):
            raise ClientException("expecting list")
        return [t["timeline_id"] for t in body]

    async def get_layer_map(self, tenant_id, timeline_id, reset) -> Tuple[Optional[str], Any]:
        resp = await self.sess.get(
            f"{self.endpoint}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer",
            params={"reset": reset},
        )
        if not resp.ok:
            raise ClientException(f"{resp}")
        launch_ts = resp.headers.get("PAGESERVER_LAUNCH_TIMESTAMP", None)
        body = await resp.json()
        return (launch_ts, body)


async def scrape_timeline(ps_id: str, ps_client: Client, db: asyncpg.Pool, tenant_id, timeline_id):
    now = datetime.datetime.utcnow()
    launch_ts, layer_map = await ps_client.get_layer_map(
        tenant_id,
        timeline_id,
        # Reset the stats on every access to get max resolution on the task kind bitmap.
        # Also, under the "every scrape does a full reset" model, it's not as urgent to
        # detect pageserver restarts in post-processing, because, to answer the question
        # "How often has the layer been accessed since its existence, across ps restarts?"
        # we can simply sum up all scrape points that we have for this layer.
        reset="AllStats",
    )
    await db.execute(
        """
            insert into layer_map (scrape_ts, pageserver_id, launch_id, tenant_id, timeline_id, layer_map)
            values ($1, $2, $3, $4, $5, $6::jsonb);""",
        now,
        ps_id,
        launch_ts,
        tenant_id,
        timeline_id,
        json.dumps(layer_map),
    )


async def timeline_task(
    args, ps_id, tenant_id, timeline_id, client: Client, db: asyncpg.Pool, stop_var: asyncio.Event
):
    """
    Task loop that is responsible for scraping one timeline
    """

    while not stop_var.is_set():
        try:
            logging.info(f"begin scraping timeline {tenant_id}/{timeline_id}")
            await scrape_timeline(ps_id, client, db, tenant_id, timeline_id)
            logging.info(f"finished scraping timeline {tenant_id}/{timeline_id}")
        except Exception:
            logging.exception(f"{tenant_id}/{timeline_id} failed, stopping scraping")
            return
        # TODO: use ticker-like construct instead of sleep()
        # TODO: bail out early if stop_var is set. That needs a select()-like statement for Python. Is there any?
        await asyncio.sleep(args.interval)


async def resolve_what(what: List[str], client: Client):
    """
    Resolve the list of "what" arguments on the command line to (tenant,timeline) tuples.
    """
    tenant_and_timline_ids: Set[Tuple[str, str]] = set()
    # fill  tenant_and_timline_ids based on spec
    for spec in what:
        comps = spec.split(":")
        if comps == ["ALL"]:
            tenant_ids = await client.get_tenant_ids()
            get_timeline_id_coros = [client.get_timeline_ids(tenant_id) for tenant_id in tenant_ids]
            gathered = await asyncio.gather(*get_timeline_id_coros, return_exceptions=True)
            assert len(tenant_ids) == len(gathered)
            for tid, tlids in zip(tenant_ids, gathered):
                for tlid in tlids:
                    tenant_and_timline_ids.add((tid, tlid))
        elif len(comps) == 1:
            tid = comps[0]
            tlids = await client.get_timeline_ids(tid)
            for tlid in tlids:
                tenant_and_timline_ids.add((tid, tlid))
        elif len(comps) == 2:
            tenant_and_timline_ids.add((comps[0], comps[1]))
        else:
            raise ValueError(f"invalid what-spec: {spec}")

    return tenant_and_timline_ids


async def main_impl(args, db: asyncpg.Pool, client: Client):
    """
    Controller loop that manages the per-timeline scrape tasks.
    """

    psid = await client.get_pageserver_id()
    scrapedb_ps_id = f"{args.environment}-{psid}"

    logging.info(f"storing results for scrapedb_ps_id={scrapedb_ps_id}")

    active_tasks_lock = asyncio.Lock()
    active_tasks: Dict[Tuple[str, str], asyncio.Event] = {}
    while True:
        try:
            desired_tasks = await resolve_what(args.what, client)
        except Exception:
            logging.exception("failed to resolve --what, sleeping then retrying")
            await asyncio.sleep(10)
            continue

        async with active_tasks_lock:
            active_task_keys = set(active_tasks.keys())

            # launch new tasks
            new_tasks = desired_tasks - active_task_keys
            for (tenant_id, timeline_id) in new_tasks:
                logging.info(f"launching scrape task for timeline {tenant_id}/{timeline_id}")
                stop_var = asyncio.Event()

                async def task_wrapper():
                    try:
                        await timeline_task(
                            args, scrapedb_ps_id, tenant_id, timeline_id, client, db, stop_var
                        )
                    finally:
                        async with active_tasks_lock:
                            del active_tasks[(tenant_id, timeline_id)]

                assert active_tasks.get((tenant_id, timeline_id)) is None
                active_tasks[(tenant_id, timeline_id)] = stop_var
                asyncio.create_task(task_wrapper())

            # signal tasks that aren't needed anymore to stop
            tasks_to_stop = active_task_keys - desired_tasks
            for (tenant_id, timeline_id) in tasks_to_stop:
                logging.info(f"stopping scrape task for timeline {tenant_id}/{timeline_id}")
                stop_var = active_tasks[(tenant_id, timeline_id)]
                stop_var.set()
                # the task will remove itself

        # sleep without holding the lock
        await asyncio.sleep(10)


async def main(args):

    dsn = f"postgres://{args.pg_user}:{args.pg_password}@{args.pg_host}/{args.pg_database}?sslmode=require"
    async with asyncpg.create_pool(dsn) as db:
        async with Client(args.endpoint) as client:
            return await main_impl(args, db, client)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    def envarg(flag, envvar, **kwargs):
        parser.add_argument(flag, default=getenv(envvar), required=not getenv(envvar), **kwargs)

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable verbose logging",
    )
    envarg("--endpoint", "SCRAPE_ENDPOINT", help="where to write report output (default: stdout)")
    envarg("--environment", "SCRAPE_ENVIRONMENT", help="environment of the pageserver")
    envarg("--interval", "SCRAPE_INTERVAL", type=int)
    envarg("--pg-host", "PGHOST")
    envarg("--pg-user", "PGUSER")
    envarg("--pg-password", "PGPASSWORD")
    envarg("--pg-database", "PGDATABASE")
    parser.add_argument(
        "what",
        nargs="+",
        help="what to download: ALL|tenant_id|tenant_id:timeline_id",
    )
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(
        format="%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d:%H:%M:%S",
        level=level,
    )

    sys.exit(asyncio.run(main(args)))
