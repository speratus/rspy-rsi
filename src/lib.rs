use pyo3::prelude::*;
use std::path::Path;
use std::borrow::Borrow;

use rusqlite::{params, Connection, Result};
use std::intrinsics::forget;

#[pymodule]
fn rspy_rsi(py: Python, m: &PyModule) -> PyResult<()> {

    m.add_class::<Word>()?;
    m.add_class::<DbConnection>()?;

    Ok(())
}

#[derive(Debug)]
#[pyclass]
struct Word {
    #[pyo3(get)]
    word: String,
    #[pyo3(get)]
    id: usize,
}

#[pyclass]
struct DbConnection {
    #[pyo3(get)]
    path: String,
}

#[pymethods]
impl DbConnection {

    #[new]
    fn new(path: String) -> Self {
        DbConnection { path }
    }

    fn load_words_in_list(&self, list: Vec<String>) -> PyResult<Vec<Word>> {
        let conn = Connection::open(self.path.borrow())?;

        let word_list = format!("({})", list_to_sql_str(list));

        let mut stmt = conn.prepare(format!("SELECT id, word FROM rss_feed_word WHERE word IN {}", word_list).as_str())?;

        let word_iter = stmt.query_map([], |row| {
            Ok(Word {
                id: row.get(0)?,
                word: row.get(1)?
            })
        });

        conn.close()?;

        Ok(word_iter.collect())
    }

    fn create_missing_words(&self, raw_list: &Vec<String>, word_list: &Vec<Word>) -> PyResult<()> {
        if word_list.len() == raw_list.len() {
            return Ok(())
        }

        let filtered: Vec<String> = raw_list.iter().filter(|s| {
            word_list.iter().any(|w| w.word == s)
        }).collect();

        let conn = Connection::open(self.path.borrow())?;

        let vals = new_word_list_to_sql(filtered);

        conn.execute(
            format!("INSERT INTO rss_feed_word (word) VALUES {}", vals).as_str(),
            params![]
        )?;

        conn.close();

        Ok(())
    }

    fn insert_w2i_data(&self, words: &Vec<Word>, item_id: &usize) -> PyResult<()> {
        let conn = Connection::open(self.path.borrow())?;

        let base_insert = "INSERT INTO rss_feed_word_rss_items (word_id, rssitem_id) VALUES {}";

        let values = word_list_to_sql_values(words, item_id);

        conn.execute(
            format!(base_insert, values).as_str(),
            params![]
        )?;

        conn.close()?;

        Ok(())
    }

    fn build_w2i_data(&self, word_list: Vec<String>, rss_item_id: usize) -> PyResult<()> {
        let words = self.load_words_in_list(word_list)?;

        self.insert_w2i_data(&words, &rss_item_id)?;
        self.create_missing_words(&word_list, &words)?;

        Ok(())
    }

}

fn list_to_sql_str(list: Vec<String>) -> String {
    let s: String = list.iter().map(|&s| format!("'{}', ", s)).collect();
    s
}

fn word_list_to_sql_values(list: &Vec<Word>, rss_id: &usize) -> String {
    let mut collector: String = String::new();
    for w in list {
        collector = format!("{} {}", collector, format!("({}, {}),", w.id, rss_id));
    }
    collector
}

fn new_word_list_to_sql(list: Vec<String>) -> String {
    let mut collector: String = String::new();
    for w in list {
        collector = format!("{} {}", collector, format!("({}),", w));
    }
    collector
}