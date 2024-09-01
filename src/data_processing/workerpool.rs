use crossbeam::channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// 定义任务类型
type Task = Box<dyn FnOnce() + Send + 'static>;

// Worker 结构体
pub struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Task>>>, running: Arc<Mutex<bool>>) -> Self {
        let thread = thread::spawn(move || loop {

                // 检查是否应该继续运行
                if !*running.lock().unwrap() {
                    // println!("Worker {} shutting down.", id);
                    break;
                }

                // 使用 recv_timeout 定期检查
                let task = receiver
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_millis(100));
                match task {
                    Ok(task) => {
                        // println!("Worker {} got a task; executing.", id);
                        task();
                    }
                    Err(_) => {
                        // recv_timeout 超时或通道关闭时触发
                        if !*running.lock().unwrap() {
                            // println!("Worker {} shutting down due to channel close.", id);
                            break;
                    }
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

// ThreadPool 结构体
pub struct WorkerPool {
    workers: Vec<Worker>,
    sender: Sender<Task>,
    running: Arc<Mutex<bool>>,
}

impl WorkerPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (sender, receiver) = unbounded();
        let receiver = Arc::new(Mutex::new(receiver));
        let running = Arc::new(Mutex::new(true));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver), Arc::clone(&running)));
        }

        WorkerPool {
            workers,
            sender,
            running,
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let task = Box::new(f);
        self.sender.send(task).unwrap();
    }

    pub fn shutdown(&self) {
        // 设置运行状态为 false，通知所有线程关闭
        let mut running = self.running.lock().unwrap();
        *running = false;

        // 关闭发送端，这样接收端会收到一个错误（`Disconnected`），从而退出循环
        drop(&self.sender);
    }

    pub fn join(&mut self) {
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown(); // 确保线程池被销毁时关闭所有 worker

        for worker in &mut self.workers {
            // println!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_pool() {
        let pool = WorkerPool::new(4);
        let counter = Arc::new(Mutex::new(0));

        for _ in 0..8 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                let mut num = counter.lock().unwrap();
                *num += 1;
                // 模拟任务处理
                std::thread::sleep(std::time::Duration::from_secs(1));
            });
        }

        // 等待所有任务完成
        std::thread::sleep(std::time::Duration::from_secs(5));

        assert_eq!(*counter.lock().unwrap(), 8);
    }
}
