# MongoDB Queue

## Lightweight C++ Job Queue and Logging library using MongoDB for queues and logs

Control flow is either by you or using boost.asio.

The MDBQ client is compatible with James Bergstra's hyperopt:

	https://github.com/jaberg/hyperopt

There is an example demonstrating this feature here:

	https://github.com/temporaer/MDBQ/blob/master/src/example/hyperopt_client.cpp


## Usage

### Own control flow:

```cpp
// in your server program instance:
Hub hub("localhost", "test.col");
hub.insert_job(BSON("foo"<<1<<"bar<<2), 1000); // timeout in 1000 seconds

// in your workers
Client clt("localhost", "test.col");
mongo::BSONObj task;
clt.get_next_task(task);
clt.log(0, BSON("progress"<<"10%"));
clt.log(0, BSON("progress"<<"50%"));
clt.checkpoint(); // flush log to server, mark that we're still working
clt.log(0, BSON("progress"<<"80%"));
clt.finish(BSON("baz"<<3));
```

### Boost.Asio control flow (preferred):

```cpp
/////////////////////////////////////////
// on hub:
/////////////////////////////////////////
boost::asio::io_service io;
Hub hub("localhost", "test.col");
// register polling of task generators
// they should be called once/periodically
// and call insert_job(...)
// You can also leave this to a 3rd party.
hub.reg(io, 1); // poll database every second
io.run();

/////////////////////////////////////////
// on client:
/////////////////////////////////////////
struct my_client : pubic mdbq::Client{
	my_client(string url, string prefix, const BSONObj& query)
	: mdbq::Client(url,prefix,query);
	void handle_task(const BSONObj& o){
		try{ /* do the task, call finish(...) */ }
		catch(mdbq::timeout_exception){/* do nothing */}
	}
};
// a client that only works on tasks with foo==1
my_client clt("localhost", "test_mdbq", BSON("foo"<<1));
clt.reg(io, 1); // poll every second
io.run();
```

## Issues:

- Clients are not killed when timeouts occur, they will get a `timeout_exception' thrown
  WHEN THEY CALL "checkpoint()". So you have to ensure that potentially
  long-running functions call checkpoint() from time to time and catch this in
  your task handler!

- Poll frequency should not be too high, since we use a remote queue. If you
  need tight loops, consider using ZMQ or the like.
