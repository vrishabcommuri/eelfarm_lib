# Eelfarm

`Eelfarm` allows distributing jobs to multiple worker instances on different 
computers. Jobs are generated in a Python environment and supplied to a 
`JobServer` instance. This instance sends the job to workers, and receives the 
results and stores them locally in `*.pickle` files. Each job is to be supplied 
in the form of functions whose output is the result. For this to work, job 
functions and their return values have to be pickleable. 


## Installing

An environment with a recent version of `Eelbrain` should contain all 
requriements. Install Eelfarm from its source folder with:

```bash
$ pip install .
```


## Running a server

A `JobServer` can be initialized with:

```python
import eelfarm
server = eelfarm.start_server()
```

The address to which workers should connect will be printed.

Jobs are supplied in the form of a function along with its arguments and a 
filename for where to save the result. 
For example (given relevant variables `y` and `x`):

```python
from eelbrain import boosting


dst = './results/job_1.pickle'
server.put(dst, boosting, y, x, 0, 0.500, error='l1')
```

This will save the result of `boosting(y, x, 0, 0.500, error='l1')` to 
`./results/job_1.pickle`.

This is all that's needed. The `JobServer` instance bound to the same address 
needs to be running in order to submit jobs to workers and receive the result, 
so the Python interpreter should not be closed. However, if the Python 
interpreter is closed and reopened, a new `JobServer` instance will still 
receive results from jobs sent in a previous session.

To see information about pending jobs, call:

```python
server.show_jobs()
```

To see information about workers that have contacted the server, call:

```python
server.show_workers()
```


## Running a worker

In order to start a worker, assuming that the primary job-server to perform work 
for has the address `192.168.1.2`, run: 

```bash
$ eelfarm start 192.168.1.2
```

This will start a worker that will perform work for `192.168.1.2` until the 
worker is terminated. By default, the worker will exist as a daemon process, 
and can be stopped again with the command:

```bash
$ eelfarm stop
```

Alternatively, the worker can be started with the `--foreground` flag, in which 
case continuous logging information will be printed on the screen, and the 
worker can be terminated with `ctrl-c`. Workers that are run with the 
`--foreground` flag can be temporarily suspended with `ctrl-z`, and resumed 
with `$ fg`.

If the job-server is the local computer (the same as running the worker), the
address can be omitted, for example:

```bash
$ eelfarm start --foreground
```

For more options see the output of:

```bash
$ eelfarm start --help
```

In general, when a worker is terminated locally, there is a risk of losing the 
current job. It is safer for the client to send a shutdown message to the 
worker, in which case ongoing jobs will be finished.
