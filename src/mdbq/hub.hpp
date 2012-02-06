#ifndef __MDBQ_HUB_HPP__
#     define __MDBQ_HUB_HPP__

#include <boost/shared_ptr.hpp>

namespace mongo
{
    class  BSONObj;
}
namespace boost{
    namespace asio
    {
        class io_service;
    }
}
namespace mdbq
{
    struct HubImpl;

    /**
     * MongoDB Queue Hub
     *
     * derive from this class to implement your own job generator
     */
    class Hub{
        private:
            /// pointer to implementation
            boost::shared_ptr<HubImpl> m_ptr;

            /// database plus queue prefix (db.queue)
            const std::string m_prefix;
        public:
            /**
             * ctor.
             *
             * @param url how to connect to mongodb
             */
            Hub(const std::string& url, const std::string& prefix);
            
            /**
             * insert job
             */
            void insert_job(const mongo::BSONObj& job, unsigned int timeout);

            /**
             * get number of pending jobs
             */
            size_t get_n_pending();

            /**
             * get number of jobs being worked on
             */
            size_t get_n_started();

            /**
             * clear the whole job queue
             */
            void clear_all();

            /**
             * determine whether new results are available.
             *
             * If there are results, they are moved to the
             * prefix+_finished collection.
             */
            size_t move_results_to_finished();

            /**
             * get number of finished items
             */
            size_t get_n_finished();

            /**
             * register with the main loop
             *
             * @param interval querying interval
             */
            void reg(boost::asio::io_service& io_service, unsigned int interval);

            virtual void got_new_results();

    };
}
#endif /* __MDBQ_HUB_HPP__ */
