use crossbeam::scope;
use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use lasso::{Key, Rodeo, RodeoReader};
use petgraph::graph::{Graph, NodeIndex};
use petgraph::prelude::*;
use petgraph::Directed;
use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::fs;
use std::io::{BufWriter, Read, Write};
use std::num::NonZeroU32;
use std::sync::RwLock;
/// The default key for every Rodeo, uses only 32 bits of space
///
/// Internally is a `NonZeroU32` to allow for space optimizations when stored inside of an [`Option`]
///
/// [`ReadOnlyLasso`]: crate::ReadOnlyLasso
/// [`Option`]: https://doc.rust-lang.org/std/option/enum.Option.html
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Spur {
    key: NonZeroU32,
}

impl Spur {
    /// Returns the [`NonZeroU32`] backing the current `Spur`
    #[cfg_attr(feature = "inline-more", inline)]
    pub const fn into_inner(self) -> NonZeroU32 {
        self.key
    }
}

unsafe impl Key for Spur {
    #[cfg_attr(feature = "inline-more", inline)]
    fn into_usize(self) -> usize {
        self.key.get() as usize - 1
    }

    /// Returns `None` if `int` is greater than `u32::MAX - 1`
    #[cfg_attr(feature = "inline-more", inline)]
    fn try_from_usize(int: usize) -> Option<Self> {
        if int < u32::max_value() as usize {
            // Safety: The integer is less than the max value and then incremented by one, meaning that
            // is is impossible for a zero to inhabit the NonZeroU32
            unsafe {
                Some(Self {
                    key: NonZeroU32::new_unchecked(int as u32 + 1),
                })
            }
        } else {
            None
        }
    }
}

impl Default for Spur {
    #[cfg_attr(feature = "inline-more", inline)]
    fn default() -> Self {
        Self::try_from_usize(0).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Triple {
    head: Spur,
    tail: Spur,
    relation: Spur,
}

fn make_spinner() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(100);
    pb
}

pub fn process(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut triples = vec![];
    let mut file = String::new();
    let mut entity_map = Rodeo::<Spur>::new();
    let mut relation_map = Rodeo::<Spur>::new();
    fs::File::open(name)?.read_to_string(&mut file)?;
    println!("Reading file...");
    let bar = make_spinner();
    for line in file.lines() {
        let mut parts = line.split_whitespace();
        let head = parts.next().ok_or("Wrong format!")?;
        let relation = parts.next().ok_or("Wrong format!")?;
        let tail = parts.next().ok_or("Wrong format!")?;
        triples.push(Triple {
            head: entity_map.get_or_intern(head),
            tail: entity_map.get_or_intern(tail),
            relation: relation_map.get_or_intern(relation),
        });
    }
    let entity_map = entity_map.into_reader();
    let relation_map = relation_map.into_reader();
    // let rodeo = entity_map.into_reader();
    bar.finish();
    println!("{} triples", triples.len());
    println!("Building maps...");
    let bar = make_spinner();
    // h, (r, t)
    // let mut one_hop_map: DashMap<Spur, Vec<(Spur, Spur)>, _> = DashMap::with_hasher_and_shard_amount(FxBuildHasher::default(), 128);
    let mut one_hop_map = (0..entity_map.len() + 1)
        .map(|_| RwLock::new(FxHashMap::<Spur, Vec<Spur>>::default()))
        .collect::<Vec<_>>();
    triples.par_iter().for_each(|triple| unsafe {
        let mut guard = one_hop_map
            .get_unchecked(triple.head.into_usize())
            .write()
            .unwrap();
        guard.entry(triple.relation).or_default().push(triple.tail);
    });
    bar.finish();
    println!("One-hop map built! Building two-hop map...");

    let one_hop_map = one_hop_map
        .into_iter()
        .filter_map(|x| x.into_inner().ok())
        .collect::<Vec<_>>();
    // h, r1, r2, t
    // let mut two_hop_map: DashMap<(Spur, Spur, Spur), FxHashSet<Spur>, _> = DashMap::with_hasher_and_shard_amount(FxBuildHasher::default(), 128);
    // let mut two_hop_map = (0..entity_map.len() + 1)
    //     .map(|_|FxHashMap::<(Spur, Spur), FxHashSet<Spur>>::default())
    //     .collect::<Vec<_>>();

    let bar = ProgressBar::new(one_hop_map.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed} / {eta}]({per_sec}) {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("#>-"),
    );
    bar.set_draw_rate(10);
    let two_hop_map: Vec<_> = one_hop_map
        .par_iter()
        .map(|rt| {
            let mut map = FxHashMap::<(Spur, Spur), Vec<Spur>>::default();
            for (r, t) in rt {
                unsafe {
                    for t in t {
                        for (r2, t2) in one_hop_map.get_unchecked(t.into_usize()) {
                            map.entry((*r, *r2)).or_insert_with(|| Vec::with_capacity(200)).extend(t2)
                        }
                    }
                }
            }
            bar.inc(1);
            map.into_iter().map(|(k, mut v)| {
                v.sort_unstable();
                v.dedup();
                (k, v.len())
            }).collect::<FxHashMap<(Spur, Spur), usize>>()
        })
        .collect();

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
        .iter()
        .map(|triple| -> (&Triple, Option<(Spur, Spur, f64)>) {
            bar.inc(1);
            unsafe {
                let tail_neighbors = one_hop_map.get_unchecked(triple.tail.into_usize());
                if tail_neighbors.len() == 0 {
                    (triple, None)
                } else {
                    let scores: Vec<_> = tail_neighbors
                        .iter()
                        .map(|(relation, tail2)| -> _ {
                            (
                                relation,
                                tail2,
                                1.0 / *two_hop_map
                                    .get_unchecked(triple.head.into_usize())
                                    .get(&(triple.relation, *relation))
                                    .unwrap() as f64,
                            )
                        })
                        .filter(|(_, _, score)| score.is_finite())
                        .collect();
                    let min_score = scores
                        .iter()
                        .max_by(|x1, x2| x1.2.total_cmp(&x2.2))
                        .unwrap();
                    let sum: f64 = scores.iter().map(|x| x.2).sum();
                    // bar.println(format!("{:?}", scores));
                    // let max_score =
                    // let sum = scores.fo
                    if (min_score.2 / sum).is_nan() {
                        bar.println(format!("{:?}", scores));
                    }
                    (
                        triple,
                        Some((
                            min_score.0.clone(),
                            min_score.1.get_unchecked(0).clone(),
                            min_score.2 / sum,
                        )),
                    )
                }
            }
        })
        .collect::<Vec<_>>()
        .iter()
        .for_each(|triple| unsafe {
            match triple {
                (triple, Some(neighbor)) => {
                    writer
                        .write(
                            format!(
                                "{}\t{}\t{}\t{}\t{}\t{}\n",
                                entity_map.resolve_unchecked(&triple.head),
                                relation_map.resolve_unchecked(&triple.relation),
                                entity_map.resolve_unchecked(&triple.tail),
                                relation_map.resolve_unchecked(&neighbor.0),
                                entity_map.resolve_unchecked(&neighbor.1),
                                neighbor.2
                            )
                            .as_bytes(),
                        )
                        .unwrap();
                }
                (triple, None) => {
                    writer
                        .write(
                            format!(
                                "{}\t{}\t{}\n",
                                entity_map.resolve_unchecked(&triple.head),
                                relation_map.resolve_unchecked(&triple.relation),
                                entity_map.resolve_unchecked(&triple.tail),
                            )
                            .as_bytes(),
                        )
                        .unwrap();
                }
            }
        });
    println!("Found neighbor!");
    Ok(())
}
