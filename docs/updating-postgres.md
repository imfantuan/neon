# Updating Postgres

## Minor Versions

When upgrading to a new minor version of Postgres, please follow these steps:

`X` _is the major version of Postgres and_ `Y` _is the new minor version._

1. Clone the Neon Postgres repository if you have not done so already.

    ```shell
    git clone git@github.com:neondatabase/postgres.git
    ```

1. Add the Postgres upstream remote.

    ```shell
    git remote add upstream https://git.postgresql.org/git/postgresql.git
    ```

1. Create a new branch based on the stable branch you are updating.

    ```shell
    git checkout -b my-branch REL_X_STABLE_neon
    ```

1. Tag the last commit on the stable branch you are updating.

    ```shell
    git tag "REL_X_$(expr $Y - 1)"
    ```

1. Find the release tags you're looking for. They are of the form `REL_X_Y`.

1. Rebase the branch you created on the tag and resolve any conflicts.

    ```shell
    git rebase REL_X_Y
    ```

1. Push your branch to the Neon Postgres repository.

    ```shell
    git push origin my-branch # or whatever you configured the remote as
    ```

1. Clone the Neon repository if you have not done so already.

    ```shell
    git clone git@github.com:neondatabase/neon.git
    ```

1. Create a new branch.

1. Change the `revisions.json` file to point at the HEAD of your Postgres
branch.

1. Update the Git submodule.

    ```shell
    git submodule set-branch --branch my-branch vendor/postgres-vX
    git submodule update --remote vendor/postgres-vX
    ```

1. Commit your changes.

1. Create a pull request.

1. Check to make sure that CI is good to go.

1. At this point, force push the rebased Postgres branches into the Neon
Postgres repository.

    ```shell
    git push --force origin my-branch:REL_X_STABLE_neon
    ```

    It may require disabling various branch protections.

1. Update your Neon PR to point at the branches.

    ```shell
    git submodule set-branch --branch REL_X_STABLE_neon vendor/postgres-vX
    git commit --amend --no-edit
    git push --force origin
    ```

1. Merge the pull request after getting approval(s) and CI completion.
