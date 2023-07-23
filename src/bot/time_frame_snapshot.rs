use chrono::Weekday;
use polars::export::num::FromPrimitive;
use serde::{Serialize, Deserialize};
#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeFrameSnapshot {
    calendar_week: i64,
    weekday: Option<i64>,
    hour: Option<i64>,
    minute: Option<i64>,
}

impl TimeFrameSnapshot {
    pub fn get_calendar_week_as_int(&self) -> i64 {
        self.calendar_week
    }

    pub fn get_weekday_as_int(&self) -> i64 {
        self.weekday.unwrap()
    }

    pub fn get_hour(&self) -> i64 {
        self.hour.unwrap()
    }

    pub fn get_minute(&self) -> i64 {
        self.minute.unwrap()
    }

    pub fn get_weekday(&self) -> Weekday {
        Weekday::from_i64(self.weekday.unwrap_or(1) - 1).unwrap()
    }

    pub fn shift_back_by_n_calendar_weeks(&self, n: i64) -> Self {
        Self {
            calendar_week: self.calendar_week - n,
            ..*self
        }
    }

    pub fn last_friday(&self) -> Self {
        Self {
            calendar_week: self.calendar_week - 1,
            weekday: Some(5),
            ..*self
        }
    }

    pub fn shift_back_by_n_weekdays(&self, n: i64) -> Self {
        Self {
            weekday: Some(self.weekday.unwrap() - n),
            ..*self
        }
    }
}

pub struct TimeFrameSnapshotBuilder {
    calendar_week: i64,
    weekday: Option<i64>,
    hour: Option<i64>,
    minute: Option<i64>,
}

impl TimeFrameSnapshotBuilder {
    pub fn new(calendar_week: i64) -> Self {
        Self {
            calendar_week,
            weekday: None,
            hour: None,
            minute: None,
        }
    }

    pub fn with_weekday(self, weekday: i64) -> Self {
        Self {
            weekday: Some(weekday),
            ..self
        }
    }

    pub fn with_hour(self, hour: i64) -> Self {
        Self {
            hour: Some(hour),
            ..self
        }
    }

    pub fn with_minute(self, minute: i64) -> Self {
        Self {
            minute: Some(minute),
            ..self
        }
    }

    pub fn build(self) -> TimeFrameSnapshot {
        TimeFrameSnapshot {
            calendar_week: self.calendar_week,
            weekday: self.weekday,
            hour: self.hour,
            minute: self.minute,
        }
    }
}
