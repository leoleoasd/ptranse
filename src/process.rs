use fxhash::{FxHashMap, FxHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use lasso::{Key, Rodeo, RodeoReader};
use petgraph::graph::{Graph, NodeIndex};
use petgraph::prelude::*;
use petgraph::Directed;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::io::{BufWriter, Read, Write};
use std::num::NonZeroU32;

// First make our key type, this will be what we use as handles into our interner
#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
struct NicheKey(NonZeroU32);

impl Default for NicheKey {
    fn default() -> Self {
        NicheKey(NonZeroU32::new(1).unwrap())
    }
}

unsafe impl Key for NicheKey {
    #[cfg_attr(feature = "inline-more", inline)]
    fn into_usize(self) -> usize {
        self.0.get() as usize - 1
    }

    /// Returns `None` if `int` is greater than `u32::MAX - 1`
    #[cfg_attr(feature = "inline-more", inline)]
    fn try_from_usize(int: usize) -> Option<Self> {
        if int < u32::max_value() as usize {
            // Safety: The integer is less than the max value and then incremented by one, meaning that
            // is is impossible for a zero to inhabit the NonZeroU32
            unsafe { Some(Self(NonZeroU32::new_unchecked(int as u32 + 1))) }
        } else {
            None
        }
    }
}

impl Into<NodeIndex> for NicheKey {
    #[cfg_attr(feature = "inline-more", inline)]
    fn into(self) -> NodeIndex {
        NodeIndex::new(self.0.get() as usize - 1)
    }
}

impl Into<EdgeIndex> for NicheKey {
    #[cfg_attr(feature = "inline-more", inline)]
    fn into(self) -> EdgeIndex {
        EdgeIndex::new(self.0.get() as usize - 1)
    }
}

impl From<NodeIndex> for NicheKey {
    #[cfg_attr(feature = "inline-more", inline)]
    fn from(index: NodeIndex) -> Self {
        NicheKey(NonZeroU32::new(index.index() as u32 + 1).unwrap())
    }
}

impl From<&NodeIndex> for NicheKey {
    #[cfg_attr(feature = "inline-more", inline)]
    fn from(index: &NodeIndex) -> Self {
        NicheKey(NonZeroU32::new(index.index() as u32 + 1).unwrap())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Triple {
    head: NicheKey,
    tail: NicheKey,
    relation: NicheKey,
}

pub fn process(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut triples = vec![];
    let mut file = String::new();
    let mut rodeo = Rodeo::<NicheKey>::new();
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
            head: rodeo.get_or_intern(head),
            tail: rodeo.get_or_intern(tail),
            relation: rodeo.get_or_intern(relation),
        });
    }
    let rodeo = rodeo.into_reader();
    bar.finish();
    println!("{} triples", triples.len());
    println!("Building maps...");
    // let head_map = DashMap::new();
    // let mut relation_map: HashMap<&str, HashMap<&str, Vec<&Triple>>> = HashMap::new();
    // triples.iter().for_each(|triple| {
    //     relation_map
    //         .entry(triple.head)
    //         .or_default()
    //         .entry(triple.relation)
    //         .or_default()
    //         .push(triple);
    // });
    // bar.finish();
    let graph = Graph::<NicheKey, NicheKey, Directed>::from_edges(
        triples.iter().map(|t| (t.head, t.tail, t.relation)),
    );
    println!("Map built with total size {}", graph.node_count());
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
        .map(|triple| -> (&Triple, Option<(Triple, f64)>) {
            bar.inc(1);
            if !graph.neighbors(triple.tail.into()).any(|x| true) {
                return (triple, None);
            }
            let mut set: FxHashMap<NicheKey, FxHashSet<NodeIndex>> = FxHashMap::default();
            for e1 in graph.edges(triple.head.into()) {
                if *e1.weight() == triple.relation {
                    for e2 in graph.edges(e1.target()) {
                        set.entry(*e2.weight()).or_default().insert(e2.target());
                    }
                }
            }
            let max = set.iter().max_by_key(|(_, v)| v.len());
            let sum = set.iter().map(|(_, v)| v.len()).sum::<usize>();
            if let Some((k, v)) = max {
                // (triple, Some((triple, v.len() as f64)))
                if v.len() == 0 {
                    (triple, Option::<(Triple, f64)>::None);
                }
                {
                    let first = v.iter().next().unwrap();
                    (
                        triple,
                        Some((
                            Triple {
                                head: triple.tail,
                                relation: *k,
                                tail: first.into(),
                            },
                            v.len() as f64 / sum as f64,
                        )),
                    )
                }
            } else {
                (triple, None)
            }
        })
        .collect::<Vec<_>>()
        .iter()
        .for_each(|triple| {
            unsafe {
                match triple {
                    (triple, Some(neighbor)) => {
                        writer
                            .write(
                                format!(
                                    "{}\t{}\t{}\t{}\t{}\t{}\n",
                                    rodeo.resolve_unchecked(&triple.head),
                                    rodeo.resolve_unchecked(&triple.relation),
                                    rodeo.resolve_unchecked(&triple.tail),
                                    rodeo.resolve_unchecked(&neighbor.0.relation),
                                    rodeo.resolve_unchecked(&neighbor.0.tail),
                                    neighbor.1
                                )
                                .as_bytes(),
                            )
                            .unwrap();
                    }
                    (triple, None) => {
                        writer.write(format!("{}\t{}\t{}\n", 
                        rodeo.resolve_unchecked(&triple.head),
                        rodeo.resolve_unchecked(&triple.relation),
                        rodeo.resolve_unchecked(&triple.tail),).as_bytes()).unwrap();
                    }
                }
            }
        });
    writer.flush()?;
    bar.finish();
    println!("Found neighbor!");
    Ok(())
}
