use pyo3::prelude::*;
use std::borrow::Borrow;

use rusqlite::{params, Connection};
use pyo3::exceptions;
use std::ops::Deref;

#[pymodule]
fn rspy_rsi(py: Python, m: &PyModule) -> PyResult<()> {

    m.add_class::<Word>()?;
    m.add_class::<DbConnection>()?;

    Ok(())
}

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
    conn: Connection,
}

#[pymethods]
impl DbConnection {

    #[new]
    fn new(path: String) -> PyResult<Self> {

        let conn;

        if let Ok(c) = Connection::open(&path) {
            conn = c;
        } else {
            return Err(exceptions::PyFileNotFoundError::new_err(format!("Could not open database at path {}", path)));
        }

        Ok(DbConnection {
            path,
            conn,
        })
    }



    fn build_w2i_data(&self, word_list: Vec<String>, rss_item_id: usize) -> PyResult<()> {
        let words = self.load_words_in_list(&word_list)?;

        self.insert_w2i_data(&words, rss_item_id)?;
        self.create_missing_words(&word_list, &words)?;

        Ok(())
    }

}

impl DbConnection {
    fn load_words_in_list(&self, list: &Vec<String>) -> PyResult<Vec<Word>> {

        let word_list = format!("({})", list_to_sql_str(list));

        let mut stmt;

        let str_stmt = format!("SELECT id, word FROM rss_feed_word WHERE word IN {}", word_list);

        if let Ok(s) = self.conn.prepare(&str_stmt) {
            stmt = s;
        } else {
            return Err(exceptions::PyBaseException::new_err(format!("Failed to construct SQL statement, SQL was: {}", str_stmt)));
        }

        let word_res_iter;

        if let Ok(iter) = stmt.query_map([], |row| {
            Ok(
                Word {
                    id: row.get(0)?,
                    word: row.get(0)?
                })
        }) {
            word_res_iter = iter;
        } else {
            return Err(exceptions::PyBaseException::new_err("Failed to bind empty params to SQL insert statement"));
        }

        let word_iter = word_res_iter.map(|word_res| {
            word_res.unwrap()
        });

        Ok(word_iter.collect())
    }

    fn create_missing_words(&self, raw_list: &Vec<String>, word_list: &Vec<Word>) -> PyResult<()> {
        if word_list.len() == raw_list.len() {
            return Ok(())
        }

        let filtered: Vec<&String> = raw_list.iter().filter(|s| {
            word_list.iter().any(|w| w.word == s.deref().deref())
        }).collect();

        let vals = new_word_list_to_sql(filtered);

        match self.conn.execute(
            format!("INSERT INTO rss_feed_word (word) VALUES {}", vals).as_str(),
            params![]
        ) {
            Ok(_) => (),
            Err(_) => return Err(exceptions::PyBaseException::new_err("Failed to execute SQL INSERT statement"))
        }

        Ok(())
    }

    fn insert_w2i_data(&self, words: &Vec<Word>, item_id: usize) -> PyResult<()> {


        let values = word_list_to_sql_values(words, &item_id);

        let insert = format!("INSERT INTO rss_feed_word_rss_items (word_id, rssitem_id) VALUES {}", values);

        match self.conn.execute(
            &insert,
            params![]
        ) {
            Ok(_) => (),
            Err(msg) => return Err(exceptions::PyBaseException::new_err(
                format!("Failed to execute SQL INSERT statement for words to items relation. Message is: {}. SQL was: {}", msg, insert)
            ))
        }

        Ok(())
    }
}

fn list_to_sql_str(list: &Vec<String>) -> String {
    let string: String = list.iter().map(|s| format!("'{}', ", s)).collect();
    let len = string.len();
    let new_len = len.saturating_sub(", ".len());

    String::from(&string[..new_len])
}

fn word_list_to_sql_values(list: &Vec<Word>, rss_id: &usize) -> String {
    let mut collector: String = String::new();
    for w in list {
        collector = format!("{} {}", collector, format!("({}, {}),", w.id, rss_id));
    }
    collector
}

fn new_word_list_to_sql(list: Vec<&String>) -> String {
    let mut collector: String = String::new();
    for w in list {
        collector = format!("{} {}", collector, format!("({}),", w));
    }
    collector
}