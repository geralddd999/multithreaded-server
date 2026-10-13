use std::{
  sync::{Arc, Mutex, mpsc::{self, Receiver}},
  thread,
};
use std::thread::available_parallelism;

pub struct ThreadPool{
  workers : Vec<Worker>,
  sender : mpsc::Sender<Job>,
}
#[derive(Debug)]
pub enum PoolCreationError{
  InvalidInput(usize),
  CoreCountFetchError,
}

struct Worker{
  id : usize,
  thread : thread::JoinHandle<()>

}

impl Worker{
  fn new(id : usize, receiver : Arc<Mutex<Receiver<Job>>>) -> Worker{
    let thread = thread::spawn(move ||{
      loop{
        let job = receiver.lock().unwrap().recv().unwrap();

        println!("Worker {id} got a job : executing...");

        job();
      }
    });
    Worker { id, thread: thread }
  }
}

type Job = Box<dyn FnOnce() + Send  + 'static>;

impl ThreadPool{
  /// Create a new ThreadPool
  ///
  /// The size is the number of threads in the pool, it also gets delimited by the quotient between the number of cores in the system divided by 2
  ///
  /// #Panics
  ///
  /// The `new` function will panic if the size is zero
  ///
  pub fn build(size : usize) -> Result<ThreadPool, PoolCreationError>{
    let cores = match available_parallelism(){
        Ok(cores) => cores.get(),
        Err(_) => return Err(PoolCreationError::CoreCountFetchError)
      };

    let size = if size < cores { size} else{ cores};
    let (sender, receiver) = mpsc::channel();
    let receiver = Arc::new(Mutex::new(receiver));

    // Report potential panics when we can't create threads!
    let threads = (0..size)
      .map(|id| Worker::new(id, Arc::clone(&receiver)))
      .collect();

    Ok(ThreadPool{
      workers : threads,
      sender : sender,
    })

  }
  pub fn new(size : usize) -> ThreadPool{
    assert!(size > 0);
    let (sender, receiver) = mpsc::channel();
    let receiver = Arc::new(Mutex::new(receiver));

    let mut threads = Vec::with_capacity(size);
    for i in 0..size{
      threads.push(Worker::new(i, Arc::clone(&receiver)));
    }

    ThreadPool { workers: threads , sender}
  }

  pub fn execute<F>(&self, f: F)
  where
    F: FnOnce() + Send + 'static,
  {
    let job = Box::new(f);
    self.sender.send(job).unwrap();
  }
}
