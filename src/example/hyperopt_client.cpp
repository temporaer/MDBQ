#include <mongo/client/dbclient.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <mdbq/client.hpp>

using namespace mdbq;

/**
 * A sample client for the quadratic1 bandit from HyperOpt.
 *
 * You should start hyperopt search like so:
 *
 * @code
 * hyperopt-mongo-search hyperopt.bandits.quadratic1 hyperopt.tpe.TreeParzenEstimator --mongo localhost/hyperopt --poll-interval=1
 * @endcode
 *
 * This client can be compiled using
 * @code
 * g++ hyperopt_client.cpp -lmdbq -lboost_thread -lboost_system -lboost_filesystem
 * @endcode
 * obviously, it requires MDBQ to be installed.
 */
struct hyperopt_client
: public Client{
    int id;
    boost::asio::io_service ios;
    hyperopt_client(int i, std::string a, std::string b)
    : Client(a,b), id(i){
    }
    void handle_task(const mongo::BSONObj& o){
        std::string cmd0 = o["cmd"].Array()[0].String();
        std::string cmd1 = o["cmd"].Array()[1].String();
        if(cmd0 != "bandit_json evaluate")
            throw std::runtime_error("HOC: do not understand cmd: `" + cmd0 +"'");
        if(cmd1 != "hyperopt.bandits.quadratic1")
            throw std::runtime_error("HOC: do not know bandit: `" + cmd1 +"'");

        float x = o["vals"]["x"].Array()[0].Double();
        float loss = ((x-3)*(x-3));
        std::cout << id <<": x = " << x << ", loss = "<< loss << std::endl;

        boost::this_thread::sleep(boost::posix_time::seconds(3));

        try{
            finish(BSON("status"<<"ok"<<"loss"<<loss));
        }catch(timeout_exception){
            std::cout <<"HOC: got timeout exception."<<std::endl;
        }
    }
    void run(){
        std::cout << "running client id:" << id << std::endl;
        this->reg(ios,1);
        ios.run();
    }
};

int
main(int argc, char **argv)
{
    // start n_clt worker threads
    static const int n_clt = 5;
    hyperopt_client* clients[n_clt];
    boost::thread*   threads[n_clt];

    for (int i = 0; i < n_clt; ++i) {
        clients[i] = new hyperopt_client(i, "localhost", "hyperopt");
        threads[i] = new boost::thread(boost::bind(&hyperopt_client::run, clients[i]));
    }

    // let them work for a while
    boost::this_thread::sleep(boost::posix_time::seconds(60));

    return 0;
}
