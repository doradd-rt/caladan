extern crate test;

use std::sync::Arc;
use std::result::Result;
use std::time::Instant;

extern crate rand;
use duration_to_ns;
use rand::Rng;
use rand_mt::Mt64;

extern crate shenango;
use shenango::Mutex;

pub enum FakeWorker {
    Ycsb(Arc<Vec<Mutex>>),
    Sqrt,
    StridedMem(Vec<u8>, usize),
    RandomMem(Vec<u8>, Vec<usize>),
    StreamingMem(Vec<u8>),
    PointerChase(Vec<usize>),
}

const ROWS_PER_TX: usize = 10;

impl FakeWorker {
    pub fn create_ycsb(lockdb: Arc<Vec<Mutex>>) -> Result<Self, &'static str> {
        Ok(FakeWorker::Ycsb(lockdb.clone()))
    }

    pub fn create(spec: &str) -> Result<Self, &str> {
        let seed: u64 = rand::thread_rng().gen();
        let mut rng: Mt64 = Mt64::new(seed);

        let tokens: Vec<&str> = spec.split(":").collect();
        assert!(tokens.len() > 0);

        match tokens[0] {
            "sqrt" => Ok(FakeWorker::Sqrt),
            "stridedmem" | "randmem" | "memstream" | "pointerchase" => {
                assert!(tokens.len() > 1);
                let size: usize = tokens[1].parse().unwrap();
                let buf = (0..size).map(|_| rng.gen()).collect();
                match tokens[0] {
                    "stridedmem" => {
                        assert!(tokens.len() > 2);
                        let stride: usize = tokens[2].parse().unwrap();
                        Ok(FakeWorker::StridedMem(buf, stride))
                    }
                    "pointerchase" => {
                        assert!(tokens.len() > 2);
                        let seed: u64 = tokens[2].parse().unwrap();
                        let mut rng: Mt64 = Mt64::new(seed);
                        let nwords = size / 8;
                        let buf: Vec<usize> =
                            (0..nwords).map(|_| rng.gen::<usize>() % nwords).collect();
                        Ok(FakeWorker::PointerChase(buf))
                    }
                    "randmem" => {
                        let sched = (0..size).map(|_| rng.gen::<usize>() % size).collect();
                        Ok(FakeWorker::RandomMem(buf, sched))
                    }
                    "memstream" => Ok(FakeWorker::StreamingMem(buf)),
                    _ => unreachable!(),
                }
            }
            _ => Err("bad fakework spec"),
        }
    }

    fn warmup_cache(&self) {
        match *self {
            FakeWorker::RandomMem(ref buf, ref sched) => {
                for i in 0..sched.len() {
                    test::black_box::<u8>(buf[sched[i]]);
                }
            }
            FakeWorker::StridedMem(ref buf, _stride) => {
                for i in 0..buf.len() {
                    test::black_box::<u8>(buf[i]);
                }
            }
            FakeWorker::PointerChase(ref buf) => {
                for i in 0..buf.len() {
                    test::black_box::<usize>(buf[i]);
                }
            }
            FakeWorker::StreamingMem(ref buf) => {
                for i in 0..buf.len() {
                    test::black_box::<u8>(buf[i]);
                }
            }
            _ => (),
        }
    }

    fn time(&self, iterations: u64) -> u64 {
        (0..50)
            .map(|_| {
                let seed: u64 = rand::thread_rng().gen();
                self.warmup_cache();
                let start = Instant::now();
                self.work(iterations, seed);
                duration_to_ns(start.elapsed())
            })
            .sum::<u64>()
            / 50
    }

    pub fn calibrate(&self, target_us: u64) {
        let target_ns = target_us * 1000;
        let mut iterations = 1;
        while self.time(iterations) < target_ns {
            iterations *= 2;
        }
        while self.time(iterations) > target_ns {
            iterations -= 1;
        }
        println!("{} us: {} iterations", target_us, iterations);
    }

    pub fn sqrt_synthetic() {
        let k = 2350845.545;
        // d6515 calibrate: 1921 for 5usec, 38413 for 100usec
        for i in 0..1921 {
            test::black_box(f64::sqrt(k * i as f64));
        }
    }

    pub fn work_ycsb(&self, indices: &mut [u32;10]) {
        match *self {
            FakeWorker::Ycsb(ref lockdb) => {
                indices.sort();
                let mut locks: Vec<&Mutex> = Vec::with_capacity(ROWS_PER_TX);
                for i in 0..ROWS_PER_TX {
                    if let Some(splock) = lockdb.get(indices[i] as usize) {
                        locks.push(&splock);
                    }
                }
        
                for i in 0..ROWS_PER_TX {
                    locks[i].lock();
                }
        
                Self::sqrt_synthetic();
        
                for i in (0..ROWS_PER_TX).rev() {
                    locks[i].unlock();
                }
            }
            FakeWorker::Sqrt => {}
            FakeWorker::StridedMem(_, _)=> {}
            FakeWorker::RandomMem(_, _) => {}
            FakeWorker::StreamingMem(..) => {}
            FakeWorker::PointerChase(..) => {}
        }
    }
        
    pub fn work(&self, iters: u64, randomness: u64) {
        match *self {
            FakeWorker::Ycsb(ref _lockdb) => {},
            FakeWorker::Sqrt => {
                let k = 2350845.545;
                for i in 0..iters {
                    test::black_box(f64::sqrt(k * i as f64));
                }
            }
            FakeWorker::StridedMem(ref buf, stride) => {
                for i in 0..iters as usize {
                    test::black_box::<u8>(buf[(randomness as usize + i * stride) % buf.len()]);
                }
            }
            FakeWorker::RandomMem(ref buf, ref sched) => {
                for i in 0..iters as usize {
                    test::black_box::<u8>(buf[sched[i % sched.len()]]);
                }
            }
            FakeWorker::PointerChase(ref buf) => {
                let mut idx = randomness as usize % buf.len();
                for _i in 0..iters {
                    idx = buf[idx];
                    test::black_box::<usize>(idx);
                }
            }
            FakeWorker::StreamingMem(ref buf) => {
                for _ in 0..iters {
                    for i in (0..buf.len()).step_by(64) {
                        test::black_box::<u8>(buf[i]);
                    }
                }
            }
        }
    }
}
