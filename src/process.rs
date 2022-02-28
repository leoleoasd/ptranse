use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::io::{BufWriter, Read, Write};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Triple<'a> {
    head: &'a str,
    tail: &'a str,
    relation: &'a str,
}
impl fmt::Display for Triple<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\t{}\t{}", self.head, self.relation, self.tail)
    }
}

pub fn process(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut triples = vec![];
    let mut file = String::new();
    fs::File::open(name)?.read_to_string(&mut file)?;
    println!("Reading file...");
    let bar = ProgressBar::new_spinner();
    bar.enable_steady_tick(100);
    for line in file.lines() {
        let mut parts = line.split_whitespace();
        let head = parts.next().ok_or("Wrong format!")?;
        let relation = parts.next().ok_or("Wrong format!")?;
        let tail = parts.next().ok_or("Wrong format!")?;
        triples.push(Triple {
            head: head,
            tail: tail,
            relation: relation,
        });
    }
    bar.finish();
    println!("{} triples", triples.len());
    println!("Building maps...");
    let bar = ProgressBar::new_spinner();
    bar.enable_steady_tick(100);
    // let head_map = DashMap::new();
    let relation_map: DashMap<&str, HashMap<&str, Vec<&Triple>>> = DashMap::new();
    triples.par_iter().for_each(|triple| {
        relation_map
            .entry(triple.head)
            .or_default()
            .entry(triple.relation)
            .or_default()
            .push(triple);
    });
    bar.finish();
    println!("Map built with total size {}", relation_map.len());
    println!("Finding neighbor...");
    let bar = ProgressBar::new(triples.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed} / {eta}]({per_sec}) {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );
    bar.set_draw_rate(10);
    let mut writer = BufWriter::new(fs::File::create(String::from(name) + "_ptranse")?);

    triples
        .par_iter()
        .map(|triple| -> (&Triple, Option<(&Triple, f64)>) {
            bar.inc(1);
            if let Some(_) = relation_map.get(triple.tail) {
                let mut map: HashMap<&str, HashMap<&str, HashSet<&Triple>>> = HashMap::new();
                relation_map.get(triple.head).map(|h| {
                    h.iter().for_each(|r1| {
                        r1.1.iter().for_each(|t1| {
                            relation_map.get(t1.tail).map(|t2| {
                                t2.iter().for_each(|r2| {
                                    // let t = ;
                                    // r2.1.iter().for_each(|t2| {
                                    //     map.entry(r1.0).or_default().entry(r2.0).or_default().insert(t2);
                                    // });
                                    map.entry(r1.0)
                                        .or_default()
                                        .entry(r2.0)
                                        .or_default()
                                        .extend(r2.1);
                                });
                            });
                        });
                    });
                });
                let total_length = map
                    .values()
                    .map(|v| v.values().map(|v| v.len()).sum::<usize>())
                    .sum::<usize>();
                // println!("{:?}", map);
                let max = map
                    .iter()
                    .filter_map(|m| {
                        // find max in m.1
                        let mm = m.1.iter().max_by_key(|d| d.1.len());
                        if let Some((r, l)) = mm {
                            Some((m.0, r, l.len()))
                        } else {
                            None
                        }
                    })
                    .max_by_key(|d| d.2);

                if let Some(r1) = max {
                    if (r1.2 == 0) {
                        (triple, None)
                    } else {
                        let t = map.get(r1.0).unwrap().get(r1.1).unwrap();
                        (
                            triple,
                            Some((
                                t.iter().take(1).next().unwrap(),
                                r1.2 as f64 / total_length as f64,
                            )),
                        )
                    }
                } else {
                    (triple, None)
                }
            } else {
                (triple, None)
            }
        })
        .collect::<Vec<_>>()
        .iter()
        .for_each(|triple| match triple {
            (triple, Some(neighbor)) => {
                writer
                    .write(
                        format!(
                            "{}\t{}\t{}\t{}\n",
                            triple, neighbor.0.relation, neighbor.0.tail, neighbor.1
                        )
                        .as_bytes(),
                    )
                    .unwrap();
            }
            (triple, None) => {
                writer.write(format!("{}\n", triple).as_bytes()).unwrap();
            }
        });
    writer.flush()?;
    bar.finish();
    println!("Found neighbor!");
    Ok(())
}
