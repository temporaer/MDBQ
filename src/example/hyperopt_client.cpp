#include <mongo/client/dbclient.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <mdbq/client.hpp>

using namespace mdbq;

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
    static const int n_clt = 5;
    hyperopt_client* clients[n_clt];
    boost::thread*   threads[n_clt];

    for (int i = 0; i < n_clt; ++i) {
        std::cout << "starting client "<<i<<std::endl;
        clients[i] = new hyperopt_client(i, "131.220.7.92", "hyperopt");
        threads[i] = new boost::thread(boost::bind(&hyperopt_client::run, clients[i]));
    }

    boost::this_thread::sleep(boost::posix_time::seconds(60));

    return 0;
}
