use regex::Regex;

#[derive(Clone)]
pub struct FilePathWithFallback {
    abs_file_path: String,
    fallback: Regex,
}

impl FilePathWithFallback {
    pub fn new(abs_file_path: String, fallback: Regex) -> Self {
        Self {
            abs_file_path,
            fallback: fallback,
        }
    }

    pub fn get_fallback_ref(&self) -> &Regex {
        &self.fallback
    }

    pub fn get_file_owned(&self) -> String {
        self.abs_file_path.clone()
    }
}
