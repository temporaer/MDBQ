#include <stdexcept>
#include <mongo/client/dbclient.h>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <mdbq/hub.hpp>
#include <mdbq/client.hpp>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE MdbQ
#include <boost/test/unit_test.hpp>


using namespace mdbq;

struct Fix{
    Hub hub;
    Client clt;
    Fix()
        :hub("localhost","test.gtest")
        ,clt("localhost","test.gtest")
    {
        hub.clear_all();
    }
};

BOOST_FIXTURE_TEST_SUITE(MdbQ, Fix)
BOOST_AUTO_TEST_CASE(create_and_destroy){
    BOOST_CHECK_EQUAL(hub.get_n_open(), 0);
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(hub.get_n_open(), 1);
}

BOOST_AUTO_TEST_CASE(client_get_task){
    BOOST_CHECK_EQUAL(0, hub.get_n_open());
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(1, hub.get_n_open());
    BOOST_CHECK_EQUAL(0, hub.get_n_assigned());

    mongo::BSONObj task;
    BOOST_CHECK(clt.get_next_task(task));
    BOOST_CHECK(!task.isEmpty());
    BOOST_CHECK_EQUAL(1, hub.get_n_assigned());
    BOOST_CHECK_EQUAL(0, hub.get_n_open());
    BOOST_CHECK_EQUAL(1, task["foo"].Int());
    BOOST_CHECK_EQUAL(2, task["bar"].Int());

    clt.finish(BSON("baz"<<3));
    BOOST_CHECK_EQUAL(0, hub.get_n_open());
    BOOST_CHECK_EQUAL(1, hub.get_n_ok());
}

BOOST_AUTO_TEST_CASE(logging){
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(1, hub.get_n_open());
    BOOST_CHECK_EQUAL(0, hub.get_n_assigned());

    mongo::BSONObj task;
    BOOST_CHECK(clt.get_next_task(task));
    BOOST_CHECK(!task.isEmpty());
    clt.log(BSON("level"<<0<<"num"<<1));
    clt.log(BSON("level"<<0<<"num"<<2));
    clt.checkpoint();
    clt.log(BSON("level"<<0<<"num"<<3));
    clt.checkpoint();

    clt.finish(BSON("baz"<<3));
    mongo::BSONObj t = hub.get_newest_finished();
    BOOST_CHECK_EQUAL(t["result"]["baz"].Int(), 3);
    BOOST_CHECK_EQUAL(t["log"].Array()[0]["level"].Int(), 0);
    BOOST_CHECK_EQUAL(t["log"].Array()[0]["num"].Int(), 1);
    BOOST_CHECK_EQUAL(t["log"].Array()[1]["level"].Int(), 0);
    BOOST_CHECK_EQUAL(t["log"].Array()[1]["num"].Int(), 2);
    BOOST_CHECK_EQUAL(t["log"].Array()[2]["level"].Int(), 0);
    BOOST_CHECK_EQUAL(t["log"].Array()[2]["num"].Int(), 3);
}

BOOST_AUTO_TEST_CASE(client_loop){
    BOOST_CHECK_EQUAL(hub.get_n_open(), 0);
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(hub.get_n_open(), 1);
    BOOST_CHECK_EQUAL(hub.get_n_assigned(), 0);

    boost::asio::io_service io;
    clt.reg(io, 1);
    hub.reg(io, 1);
    io.run();
}
BOOST_AUTO_TEST_SUITE_END()
