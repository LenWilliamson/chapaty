use std::collections::HashSet;

use chrono::{NaiveDate, NaiveTime};
use strum_macros::{Display, EnumIter};

use crate::converter::timeformat::naive_date_from_str;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EnumIter, Display)]
pub enum NewsKind {
    UsaNFP,
    UsaCPI,
}

impl NewsKind {
    pub fn get_news_dates(&self) -> HashSet<NaiveDate> {
        match self {
            Self::UsaNFP => usa_nfp_news_dates(),
            Self::UsaCPI => usa_cpi_news_dates(),
        }
    }

    // pub fn get_news_dates_with_time(&self) -> HashSet<NaiveDateTime> {
    //     match self {
    //         Self::UsaNFP => usa_nfp_news_with_utc_time(&self.utc_time()),
    //         Self::UsaCPI => usa_cpi_news_with_utc_time(&self.utc_time()),
    //     }
    // }

    pub fn utc_time(&self) -> NaiveTime {
        match self {
            Self::UsaCPI => NaiveTime::from_hms_opt(12, 30, 0).unwrap(),
            Self::UsaNFP => NaiveTime::from_hms_opt(12, 30, 0).unwrap(),
        }
    }
}

fn usa_nfp_news_dates() -> HashSet<NaiveDate> {
    vec![
        "2006-01-06",
        "2006-02-03",
        "2006-03-10",
        "2006-04-07",
        "2006-05-05",
        "2006-05-08",
        "2006-06-02",
        "2006-07-07",
        "2006-08-04",
        "2006-09-01",
        "2006-10-06",
        "2006-11-03",
        "2006-12-08",
        "2007-01-05",
        "2007-02-02",
        "2007-03-09",
        "2007-04-06",
        "2007-05-04",
        "2007-06-01",
        "2007-07-06",
        "2007-08-03",
        "2007-09-07",
        "2007-10-05",
        "2007-11-02",
        "2007-12-07",
        "2008-01-04",
        "2008-02-01",
        "2008-03-07",
        "2008-04-04",
        "2008-05-02",
        "2008-06-06",
        "2008-07-03",
        "2008-08-01",
        "2008-09-05",
        "2008-10-03",
        "2008-11-07",
        "2008-12-05",
        "2009-01-09",
        "2009-02-06",
        "2009-03-06",
        "2009-04-03",
        "2009-05-08",
        "2009-06-05",
        "2009-07-02",
        "2009-08-07",
        "2009-09-04",
        "2009-10-02",
        "2009-11-06",
        "2009-12-04",
        "2010-01-08",
        "2010-02-05",
        "2010-03-05",
        "2010-04-02",
        "2010-05-07",
        "2010-06-04",
        "2010-07-02",
        "2010-08-06",
        "2010-09-03",
        "2010-10-08",
        "2010-11-05",
        "2010-12-03",
        "2011-01-07",
        "2011-02-04",
        "2011-03-04",
        "2011-04-01",
        "2011-05-06",
        "2011-06-03",
        "2011-07-08",
        "2011-08-05",
        "2011-09-02",
        "2011-10-07",
        "2011-11-04",
        "2011-12-02",
        "2012-01-06",
        "2012-02-03",
        "2012-03-09",
        "2012-04-06",
        "2012-05-04",
        "2012-06-01",
        "2012-07-06",
        "2012-08-03",
        "2012-09-07",
        "2012-10-05",
        "2012-11-02",
        "2012-12-07",
        "2012-12-12",
        "2013-01-04",
        "2013-02-01",
        "2013-03-08",
        "2013-04-05",
        "2013-05-03",
        "2013-05-06",
        "2013-06-07",
        "2013-07-05",
        "2013-08-02",
        "2013-09-06",
        "2013-10-22",
        "2013-11-08",
        "2013-12-06",
        "2014-01-10",
        "2014-02-07",
        "2014-03-07",
        "2014-04-04",
        "2014-05-02",
        "2014-06-06",
        "2014-07-03",
        "2014-08-01",
        "2014-09-05",
        "2014-10-03",
        "2014-11-07",
        "2014-12-05",
        "2015-01-09",
        "2015-02-06",
        "2015-03-06",
        "2015-04-03",
        "2015-05-08",
        "2015-06-05",
        "2015-07-02",
        "2015-08-07",
        "2015-09-04",
        "2015-10-02",
        "2015-11-06",
        "2015-12-04",
        "2016-01-08",
        "2016-02-05",
        "2016-03-04",
        "2016-04-01",
        "2016-05-06",
        "2016-06-03",
        "2016-07-08",
        "2016-08-05",
        "2016-09-02",
        "2016-10-07",
        "2016-11-04",
        "2016-12-02",
        "2017-01-06",
        "2017-02-03",
        "2017-03-10",
        "2017-04-07",
        "2017-05-05",
        "2017-06-02",
        "2017-07-07",
        "2017-08-04",
        "2017-09-01",
        "2017-10-06",
        "2017-11-03",
        "2017-12-08",
        "2018-01-05",
        "2018-02-02",
        "2018-03-09",
        "2018-04-06",
        "2018-05-04",
        "2018-06-01",
        "2018-07-06",
        "2018-08-03",
        "2018-09-07",
        "2018-10-05",
        "2018-11-02",
        "2018-12-07",
        "2019-01-04",
        "2019-02-01",
        "2019-03-08",
        "2019-04-05",
        "2019-05-03",
        "2019-06-07",
        "2019-07-05",
        "2019-08-02",
        "2019-09-06",
        "2019-10-04",
        "2019-11-01",
        "2019-12-06",
        "2020-01-10",
        "2020-02-07",
        "2020-03-06",
        "2020-04-03",
        "2020-05-08",
        "2020-05-11",
        "2020-06-05",
        "2020-07-02",
        "2020-08-07",
        "2020-09-04",
        "2020-10-02",
        "2020-11-06",
        "2020-12-04",
        "2021-01-08",
        "2021-02-05",
        "2021-03-05",
        "2021-04-02",
        "2021-05-07",
        "2021-06-04",
        "2021-07-02",
        "2021-08-06",
        "2021-09-03",
        "2021-10-08",
        "2021-11-05",
        "2021-12-03",
        "2022-01-07",
        "2022-02-04",
        "2022-03-04",
        "2022-04-01",
        "2022-05-06",
        "2022-06-03",
        "2022-07-08",
        "2022-08-05",
        "2022-09-02",
        "2022-10-07",
        "2022-11-04",
        "2022-12-02",
        "2023-01-06",
        "2023-02-03",
        "2023-03-10",
        "2023-04-07",
        "2023-05-05",
        "2023-06-02",
        "2023-07-07",
        "2023-08-04",
        "2023-09-01",
        "2023-10-06",
        "2023-11-03",
    ]
    .iter()
    .map(|date| naive_date_from_str(date, "%d-%m-%Y"))
    .collect()
}

fn usa_cpi_news_dates() -> HashSet<NaiveDate> {
    vec![
        "2006-01-18",
        "2006-02-17",
        "2006-02-22",
        "2006-03-16",
        "2006-04-19",
        "2006-05-17",
        "2006-06-14",
        "2006-07-19",
        "2006-08-16",
        "2006-09-15",
        "2006-10-18",
        "2006-11-16",
        "2006-12-15",
        "2007-01-18",
        "2007-02-16",
        "2007-02-21",
        "2007-03-16",
        "2007-04-17",
        "2007-05-15",
        "2007-06-15",
        "2007-07-18",
        "2007-08-15",
        "2007-09-19",
        "2007-10-17",
        "2007-11-15",
        "2007-12-14",
        "2008-01-16",
        "2008-02-15",
        "2008-02-20",
        "2008-03-14",
        "2008-04-16",
        "2008-05-14",
        "2008-06-13",
        "2008-07-16",
        "2008-08-14",
        "2008-09-16",
        "2008-10-16",
        "2008-11-19",
        "2008-12-16",
        "2009-01-16",
        "2009-02-18",
        "2009-02-20",
        "2009-03-18",
        "2009-04-15",
        "2009-05-15",
        "2009-06-17",
        "2009-07-15",
        "2009-08-14",
        "2009-09-16",
        "2009-10-15",
        "2009-11-18",
        "2009-12-16",
        "2010-01-15",
        "2010-02-17",
        "2010-02-19",
        "2010-03-18",
        "2010-04-14",
        "2010-05-19",
        "2010-06-17",
        "2010-07-16",
        "2010-08-13",
        "2010-09-17",
        "2010-10-15",
        "2010-11-17",
        "2010-12-15",
        "2011-01-14",
        "2011-02-15",
        "2011-02-17",
        "2011-03-17",
        "2011-04-15",
        "2011-05-13",
        "2011-06-15",
        "2011-07-15",
        "2011-08-18",
        "2011-09-15",
        "2011-10-19",
        "2011-11-16",
        "2011-12-16",
        "2012-01-19",
        "2012-02-15",
        "2012-02-17",
        "2012-03-16",
        "2012-04-13",
        "2012-05-15",
        "2012-06-14",
        "2012-07-17",
        "2012-08-15",
        "2012-09-14",
        "2012-10-16",
        "2012-11-15",
        "2012-12-14",
        "2013-01-16",
        "2013-02-19",
        "2013-02-21",
        "2013-03-15",
        "2013-04-16",
        "2013-05-16",
        "2013-06-18",
        "2013-07-16",
        "2013-08-15",
        "2013-09-17",
        "2013-10-30",
        "2013-11-20",
        "2013-12-17",
        "2014-01-16",
        "2014-02-18",
        "2014-02-20",
        "2014-03-18",
        "2014-04-15",
        "2014-05-15",
        "2014-06-17",
        "2014-07-22",
        "2014-08-19",
        "2014-09-17",
        "2014-10-22",
        "2014-11-20",
        "2014-12-17",
        "2015-01-16",
        "2015-02-20",
        "2015-02-26",
        "2015-03-24",
        "2015-04-17",
        "2015-05-22",
        "2015-06-18",
        "2015-07-17",
        "2015-08-19",
        "2015-09-16",
        "2015-10-15",
        "2015-11-17",
        "2015-12-15",
        "2016-01-20",
        "2016-02-19",
        "2016-03-16",
        "2016-04-14",
        "2016-05-17",
        "2016-06-16",
        "2016-07-15",
        "2016-08-16",
        "2016-09-16",
        "2016-10-18",
        "2016-11-17",
        "2016-12-15",
        "2017-01-18",
        "2017-02-13",
        "2017-02-15",
        "2017-03-15",
        "2017-04-14",
        "2017-05-12",
        "2017-06-14",
        "2017-07-14",
        "2017-08-11",
        "2017-09-14",
        "2017-10-13",
        "2017-11-15",
        "2017-12-13",
        "2018-01-12",
        "2018-02-14",
        "2018-03-13",
        "2018-04-11",
        "2018-05-10",
        "2018-06-12",
        "2018-07-12",
        "2018-08-10",
        "2018-09-13",
        "2018-10-11",
        "2018-11-14",
        "2018-12-12",
        "2019-01-11",
        "2019-02-11",
        "2019-02-13",
        "2019-03-12",
        "2019-04-10",
        "2019-05-10",
        "2019-06-12",
        "2019-07-11",
        "2019-08-13",
        "2019-09-12",
        "2019-10-10",
        "2019-11-13",
        "2019-12-11",
        "2020-01-14",
        "2020-02-11",
        "2020-02-13",
        "2020-03-11",
        "2020-04-10",
        "2020-05-12",
        "2020-06-10",
        "2020-07-14",
        "2020-08-12",
        "2020-09-11",
        "2020-10-13",
        "2020-11-12",
        "2020-12-10",
        "2021-01-13",
        "2021-02-08",
        "2021-02-10",
        "2021-03-10",
        "2021-04-13",
        "2021-05-12",
        "2021-06-10",
        "2021-07-13",
        "2021-08-11",
        "2021-09-14",
        "2021-10-13",
        "2021-11-10",
        "2021-12-10",
        "2022-01-12",
        "2022-02-08",
        "2022-02-10",
        "2022-03-10",
        "2022-04-12",
        "2022-05-11",
        "2022-06-10",
        "2022-07-13",
        "2022-08-10",
        "2022-09-13",
        "2022-10-13",
        "2022-11-10",
        "2022-12-13",
        "2023-01-12",
        "2023-02-10",
        "2023-02-14",
        "2023-03-14",
        "2023-04-12",
        "2023-05-10",
        "2023-06-13",
        "2023-07-12",
        "2023-08-10",
        "2023-09-13",
        "2023-10-12",
    ]
    .iter()
    .map(|date| naive_date_from_str(date, "%d-%m-%Y"))
    .collect()
}

// fn usa_nfp_news_with_utc_time(time: &NaiveTime) -> HashSet<NaiveDateTime> {
//     usa_nfp_news_dates()
//         .iter()
//         .map(|date| date.and_time(*time))
//         .collect()
// }

// fn usa_cpi_news_with_utc_time(time: &NaiveTime) -> HashSet<NaiveDateTime> {
//     usa_cpi_news_dates()
//         .iter()
//         .map(|date| date.and_time(*time))
//         .collect()
// }
