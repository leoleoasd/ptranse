use fxhash::{FxHashMap};
use indicatif::{ParallelProgressIterator,ProgressIterator, ProgressBar, ProgressStyle};
use lasso::{Key, Rodeo};
use rayon::prelude::*;
use std::fmt::Debug;
use std::io::{BufWriter, Read, Write};
use std::num::NonZeroU32;
use std::ops::Add;
use std::sync::RwLock;
use std::{fs, io};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Spur {
    key: u32,
}

unsafe impl Key for Spur {
    #[cfg_attr(feature = "inline-more", inline)]
    fn into_usize(self) -> usize {
        self.key as usize
    }

    /// Returns `None` if `int` is greater than `u32::MAX - 1`
    #[cfg_attr(feature = "inline-more", inline)]
    fn try_from_usize(int: usize) -> Option<Self> {
        if int < u32::max_value() as usize {
            Some(Self{key: int as u32})
        } else {
            None
        }
    }
}

impl From<u32> for Spur {
    #[cfg_attr(feature = "inline-more", inline)]
    fn from(key: u32) -> Self {
        Self {
            key,
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

fn make_bar(len: usize) -> ProgressBar {
    let bar = ProgressBar::new(len as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed} / {eta}]({per_sec}) {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("#>-"),
    );
    bar.set_draw_rate(10);
    bar
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
    let one_hop_map = (0..=entity_map.len())
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
    drop(bar);

    let one_hop_map = one_hop_map
        .into_iter()
        .filter_map(|x| x.into_inner().ok())
        .collect::<Vec<_>>();

    println!("One-hop map built! Building two hot map...");
        
    let two_hop_map: Vec<_> = one_hop_map
        .par_iter()
        .progress_with(make_bar(one_hop_map.len()))
        .map(|rt| {
            let mut map = FxHashMap::<(Spur, Spur), Vec<Spur>>::default();
            for (r, t) in rt {
                unsafe {
                    for t in t {
                        for (r2, t2) in one_hop_map.get_unchecked(t.into_usize()) {
                            map.entry((*r, *r2))
                                .or_insert_with(|| Vec::with_capacity(200))
                                .extend(t2)
                        }
                    }
                }
            }
            map.into_iter()
                .map(|(k, mut v)| {
                    v.sort_unstable();
                    v.dedup();
                    (k, v.len())
                })
                .collect::<FxHashMap<(Spur, Spur), usize>>()
        })
        .collect();

    println!("Two-hop map built! Building path map...");

    // h,t -> <r1, r2>
    let path_map: Vec<FxHashMap<Spur, Vec<(Spur, Spur)>>> = one_hop_map
        .par_iter()
        .progress_with(make_bar(one_hop_map.len()))
        .map(|rt| {
            // t, <r1, r2>
            let mut map = FxHashMap::<Spur, Vec<(Spur, Spur)>>::default();
            for (r, t) in rt {
                unsafe {
                    for t in t {
                        for (r2, t2) in one_hop_map.get_unchecked(t.into_usize()) {
                            for tt in t2 {
                                map.entry(*tt).or_default().push((*r, *r2));
                            }
                        }
                    }
                }
            }
            map.into_iter()
                .map(|(t, mut r)| {
                    r.sort_unstable();
                    r.dedup();
                    (t, r)
                })
                .collect()
        })
        .collect();

    println!("Finding neighbor...");
    let bar = make_bar(triples.len());
    let mut writer = BufWriter::new(fs::File::create(String::from(name) + "_ptranse")?);
    let mut lengthes = FxHashMap::<usize, usize>::default();

    triples
        .iter()
        .map(|triple| {
            bar.inc(1);
            unsafe {
                // [(r1, r2)]
                let mut candi: Vec<_> = path_map
                    .get_unchecked(triple.head.into_usize())
                    .get(&triple.tail)
                    .map_or_else(Vec::<((Spur, Spur), f64)>::new, |x| {
                        x.iter()
                        .map(|(r1, r2)| {
                            // get <h, r1, r2>'s count
                            let count = *two_hop_map
                                .get_unchecked(triple.head.into_usize())
                                .get(&(*r1, *r2))
                                .unwrap();
                            ((*r1, *r2), 1.0 / count as f64)
                        })
                        .collect()
                    });
                *lengthes.entry(candi.len()).or_insert(0) += 1;
                candi.sort_unstable_by(|a, b| {
                    // should not be NAN
                    b.1.partial_cmp(&a.1).unwrap()
                });
                candi.truncate(5);
                while candi.len() < 5 {
                    candi.push(((0.into(), 0.into()), 0.0));
                }
                (triple, candi)
            }
        })
        .try_for_each(|(triple, candi)| -> Result<(), io::Error> {
            if candi.len() != 5 {
                panic!("WTF? {}", candi.len());
            }
            unsafe {
                write!(
                    writer,
                    "{}\t{}\t{}",
                    entity_map.resolve_unchecked(&triple.head),
                    relation_map.resolve_unchecked(&triple.relation),
                    entity_map.resolve_unchecked(&triple.tail)
                )?;
                for cand in candi {
                    write!(
                        writer,
                        "\t{}\t{}\t{}",
                        relation_map.resolve_unchecked(&cand.0.0),
                        relation_map.resolve_unchecked(&cand.0.1),
                        cand.1
                    )?;
                }
            }
            writeln!(writer)?;
            Ok(())
        })?;
    for (k,v) in lengthes {
        println!("length of {} has {}", k, v);
    }
    println!("Found neighbor!");
    Ok(())
}
