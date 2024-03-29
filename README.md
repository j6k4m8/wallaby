# wallaby 🦘

A centralized, single landing-spot for logs from multiple parallel jobs.

Imagine:

You have a bunch of jobs running on a cluster. You want to collect output from all of them, but writing to files is a pain, and writing to a database requires schema design and config.

---

Wallaby is a simple, schemaless output-gobbler for collecting data from parallel jobs on a cluster or from a series of parallel jobs on a single machine. There are NO DEPENDENCIES besides Python.

Primary use-cases:

-   I want to run a bunch of jobs on my cluster, but I hate having to deal with thousands of "jobname.####.out" and "jobname.####.err" files
-   I want to collect output from jobs in a workflow, but it's a pain to orchestrate all that STDIN/STDOUT
-   I'm running a cron job and I don't have a good home for my outputs
-   I'm running a hyperparameter search and want to keep track of parameters' performance across nodes or cores

You can use Wallaby either in module-mode, or in command-line mode.

## Command-line Mode

You can pass stdout to Wallaby, or, more ideally, you can use Wallaby as your command-runner.

**Best:**

```shell
wallaby --command 'ls -lh'
```

This will generate a data row that contains environment variables, the command you ran, and the output.

**Second-best:**

```shell
ls -lh | wallaby
```

This will do the same as the above, but you will not have access to the command that you ran to get the results. In other words, the `command` field will be left blank. This is because you cannot access the name of a forked command from inside a downstream pipe. So if at all possible, run the `--command`-flag version!

## Other Arguments

You can also pass a `--tags` CSV to add a set of tags to the new record. For example:

```shell
wallaby --tags foo,bar,elephant --command 'ls -lah'
```

## Module Mode

```python
from wallaby import Wallaby

w = Wallaby()

w.log("new results: the answer is 42")
```

You can also point to a specific Wallaby database with:

```python
w = Wallaby("/path/to/database")
```

You can pass a dictionary of results as well:

```python
w.log({
    "answer": 42,
    "favorite_planet": "Brontitall"
})
```

If you pass a string, it is assumed that you want the following structure:

```json
{
    "output": "new results: the answer is 42"
}
```

## Tag Organization

All Wallaby logs can be associated with tags. You can then query the database based upon these tags:

```python
w.log("at 4pm today my cron job succeeded!", tags=["cron", "my_favorite_cron"])
```

## Querying Results

You can get a list of all results based upon tags, using either set intersect or set union:

### Querying by Tag

#### Get all results tagged with 'foo'

```python
w.get_by_tag("foo")
```

#### Union: (Results tagged with 'foo') ∪ (Results tagged with 'bar')

```python
w.get_by_tag(any_of=["foo", "bar"])
```

#### Intersect: (tagged with 'foo') ∩ (tagged with 'bar') ∩ (tagged with 'baz')

```python
w.get_by_tag(all_of=["foo", "bar", "baz"])
```

### Query by creation time

#### Get results created in the past hour (60\*60 seconds)

```python
w.get_results_since(time.time() - 60*60)
```

## Examples

### Example with [frof](https://github.com/j6k4m8/frof/)

Your current frof file:

```yml
setup -> run_for_all_patients(&pt_id) -> cleanup

setup:                  ...
run_for_all_patients:   python3 analyze-patient.py
cleanup:                ...

&pt_id: [100, 204, 854, 2955, 4862]
```

Your new frof file:

```yml
setup -> run_for_all_patients(&pt_id) -> cleanup

setup:                  ...
run_for_all_patients:   wallaby --command 'python3 analyze-patient.py'
cleanup:                ...

&pt_id: [100, 204, 854, 2955, 4862]
```

Now all outputs of the analyze-patient step will be collected for later.

### Example with slurm

Your current slurm command file:

```bash
#SBATCH --array=0-100

bash ./run-my-tool.sh
```

Your new slurm command file:

```bash
#SBATCH --array=0-100

wallaby --command 'bash ./run-my-tool.sh'
```


# Collecting Results with `wallaby2json`

The easiest way to collect results is with the `wallaby2json` tool, which converts saved results into a pandas-compatible JSON file.


```bash
wallaby2json -t foo,bar > results.json
```

The `-t` flag forwards to the `all_of` tag filter.
