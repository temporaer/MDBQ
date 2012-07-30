#ifndef __MDBQ_DATE_TIME_HPP__
#     define __MDBQ_DATE_TIME_HPP__
#include <cassert>
#include <vector>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <mongo/client/dbclient.h>
#include <mongo/client/gridfs.h>

namespace mdbq
{
    inline
    boost::posix_time::ptime
    universal_date_time(){
        boost::posix_time::time_duration dur(boost::posix_time::microsec_clock::universal_time().time_of_day());
        boost::gregorian::date date(boost::gregorian::day_clock::universal_day());
        return boost::posix_time::ptime(date,dur);
    }
    inline
    mongo::BSONArray
    ptime_to_bson(const boost::posix_time::ptime& pt){
        boost::posix_time::time_duration dur(pt.time_of_day());
        boost::gregorian::date date(pt.date());
        double f = dur.fractional_seconds();
        int millisec = (int)(f*1000);
        long long int microsec = 1000 * (f - millisec/1000.f);
        return BSON_ARRAY(
                date.year() << date.month() << date.day() <<
                dur.hours() << dur.minutes() << dur.seconds() << millisec <<
                microsec
                );
    }

    inline
    boost::posix_time::ptime 
    bson_to_ptime(const mongo::BSONElement& elem){
        //assert(elem.isABSONObj());
        //assert(elem.type() == mongo::Array);
        std::vector<mongo::BSONElement> arr = elem.Array();

        boost::posix_time::ptime p(
                boost::gregorian::date(
                    arr[0].Int(), arr[1].Int(), arr[2].Int()),
                boost::posix_time::time_duration(
                    arr[3].Int(), arr[4].Int(), arr[5].Int(),
                    arr[6].Int()/1000.f+arr[7].Long()/1000000.f));
        return p;
    }

    inline
    std::string
    dt_format(const boost::posix_time::ptime& t){
        std::stringstream ss;
        ss<<t.date().month()
          <<"-"             <<t.date().day()
          <<" "             <<t.time_of_day().hours()
          <<":"             <<t.time_of_day().minutes()
          <<":"             <<t.time_of_day().seconds();
        return ss.str();
    }
}

#endif /* __MDBQ_DATE_TIME_HPP__ */
