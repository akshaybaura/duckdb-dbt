# SQS Data Pipeline

In this exercise, you are expected to develop a simple ETL tool. The tool should consume messages from an AWS SQS queue 
and then store them in a database of your choice with a structure you defined based on the requirements below.

Expectations from you are:

* Research and understand how AWS SQS works on a basic level
* Submit a working solution
* Provide documentation (explained in `Documentation` section)
* Examine data analytic team query requirements
* Create a suitable database schema to facilitate analytic queries
* Transform incoming events into the structure you defined
* Provide SQL queries for data analytic team's requirements

## Localstack
https://github.com/localstack/localstack

Instead of using an actual AWS account, you will use a localstack implementation to imitate AWS services. Localstack
uses the same API with AWS services, you can use the same API by targeting the local endpoint (stated in the
tool documentation).

## Installation Requirements
- [docker](https://www.docker.com/get-started)
- [docker-compose](https://docs.docker.com/compose/install/)

## Input

### Environment setup

In this exercise you have three files:
- `docker-compose.yml` 
- `message-generator`
- `README.md`

To setup the localstack environment, run:
```bash
$ docker-compose up
```

To setup the test case, you can run `message-generator`s appropriate for your environment. This command will load input event messages into your local setup.
 Right now darwin, linux, and windows OSs are supported:

```bash
$ ls message-generators
darwin       linux        windows.exe
$ ./message-generators/linux    # for linux
$ ./message-generators/darwin   # for macos
```

**P.S.**: You can run `message-generator` more than once, just be careful with event duplication.

**P.S.**: You can use source code in `message-generators/code` to run the message generator by using `go run message_generator.go` in the same directory if you face any problem with compiled binaries. (You should install golang for this)

Follow the outputs to configure your tool for SQS queue URL.


## Scenario

Your department is analyzing development activity in your organization's git repositories.
The event stream is provided by software engineers already. Your team consumes `PullRequest` and `Issue` events to answer business inquiries.

Data analytics team needs your help to transform these two event structures into a single event type.

### Data analytics team requirements

You will receive events with two different structures: `PullRequest` and `Issue`. Please prepare a table structure for one singular event structure considering the queries below:

 - List the most active users 
 - List longest open event (for `Issue` from `started_at` to `closed_at` for `PullRequest` from `created_at` to `merged_at` )
 - List the most popular five tags for all repositories (or `label` for `Issue`)
 - List the total completed event count per repository for a given period
 - List top users based on number of repositories they contributed 

### Event structures

- Issue Structure:

```json
{
  "type": "issue",
  "id": 1455829643,
  "state": "closed",
  "title": "Lorem Ipsum Lorem Ipsum",
  "body": "",
  "user": "loremipsum",
  "labels": [
    {
      "id": 1738049645,
      "name": "in: documentation"
    },
    {
      "id": 1738049962,
      "name": "type: bug"
    }
  ],
  "closed_at": "2022-11-19T07:48:01Z",
  "started_at": "2022-11-18T21:25:48Z",
  "repository": "https://api.github.com/repos/spring-projects/spring-batch"
}
```

- PullRequest structure:

```json
{
    "type": "pull-requests",
    "id": 483968200,
    "status": "closed",
    "title": "Lorem Ipsum Lorem Ipsum",
    "user": "loremipsum",
    "body": "",
    "tags": [
      {
        "id": 1738049962,
        "name": "type: bug"
      }
    ],
    "merged_at": 1599749218,
    "created_at": 1599739234,
    "repository": {
      "name": "spring-integration",
      "owner": "spring-projects"
    }
  }
```

All values here are examples, you need to get actual values from the queue messages.

## Output


### Persisting
You need to persist the transformed events. Please make sure you consumed all messages, there are more than 50k messages.


Please setup a local database, that can be reproducible in our systems as well (both Linux and Darwin). We prefer to have a `docker run`
command to run the database of your choice.

### Language
Please prepare your solution with your language of choice. However, you are encouraged to use one of:

- Java/Kotlin/Scala
- Go
- Python 3


Please motivate your choice of programming language in the documentation.

### Documentation
You are expected to provide the source code and a documentation of the tool. Please add a `DOCUMENTATION.md` file in
you submission which includes:

- How to run your tool, provide a Makefile if necessary
- Your proposed final event structure
- SQL queries for above data analytics team's requirements

### Submission format
Please open a Pull/Merge request to this repository.

```
$ tree .
├── DOCUMENTATION.md
└── src
    └── ...
```
Please include `DOCUMENTATION.md`, source code, and build scripts if necessary to your submission.

Your submission should be able to run with a single command. You can add a Makefile script that runs required commands if needed. See Bonus Points #3.

--

Your program will be judged on the quality of the code as well as the correctness of the output.

## Bonus points
1. Include your database setup to `docker-compose` setup as container.
2. Make your tool runnable by docker. Provide running instructions.
3. Prepare a Makefile that builds your submission and runs it.
4. Provide an open-source tool that can replace your custom implementation, briefly discuss why it is a good choice for this case
