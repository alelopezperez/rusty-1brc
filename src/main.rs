use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    thread::{self, available_parallelism},
};

fn create_fd(path: &str, num_cores: u64, chunk_size: u64, file_size: u64) -> Vec<(u64, u64)> {
    let mut fd_vec = vec![];

    let mut buf_reader = BufReader::new(File::open(path).unwrap());
    let mut start = 0;
    let mut end;

    for _ in 1..num_cores {
        end = start + chunk_size;

        buf_reader.seek(SeekFrom::Start(end)).unwrap();

        let mut ch = [b'0'];
        buf_reader.read_exact(&mut ch).unwrap();

        while ch[0] as char != '\n' {
            buf_reader.read_exact(&mut ch).unwrap();
        }

        end = buf_reader.stream_position().unwrap();
        fd_vec.push((start, end));
        start = end;
    }

    fd_vec.push((start, file_size));
    fd_vec
}

fn ranges_test(ran: Vec<(u64, u64)>) {
    let file = File::open("measurements.txt").unwrap();
    let mut reader = BufReader::new(file);
    let mut line = String::new();

    let (start, end) = ran[0];
    reader.seek(SeekFrom::Start(start)).unwrap();

    let mut reader = reader.take(end - start);
    let mut count = 0;
    while reader.read_line(&mut line).unwrap() != 0 {
        count += 1;
        line.clear();
    }
    println!("{count}");

    // Repeat

    let file = File::open("measurements.txt").unwrap();
    let mut reader = BufReader::new(file);
    let mut line = String::new();

    let (start, end) = ran[1];
    reader.seek(SeekFrom::Start(start)).unwrap();

    let mut reader = reader.take(end - start);
    let mut count = 0;
    while reader.read_line(&mut line).unwrap() != 0 {
        count += 1;
        line.clear();
    }
    println!("{count}");
}
fn main() {
    let num_cores = available_parallelism().unwrap().get() as u64;
    let file = File::open("measurements.txt").unwrap();
    let file_size = file.metadata().unwrap().size();
    let chunk_size = file_size / num_cores;

    println!("cores: {num_cores}");
    println!("file size: {file_size}");
    println!("chunks: {chunk_size}");

    let ranges = create_fd("measurements.txt", num_cores, chunk_size, file_size);

    let mut handles = Vec::new();

    println!("Starting threads");

    for (i, range) in ranges.into_iter().enumerate() {
        let handle = thread::spawn(move || {
            let file = File::open("measurements.txt").unwrap();
            let mut reader = BufReader::new(file);
            let mut line = String::new();
            let (start, end) = range;
            let length = end - start;
            let mut measurements_map = HashMap::<String, (f32, f32, f32, i64)>::new();

            reader.seek(SeekFrom::Start(start)).unwrap();
            let mut reader = reader.take(length);
            let mut bilion: u64 = 0;

            while reader.read_line(&mut line).expect("Should not Fail") != 0 {
                bilion += 1;
                let (name, temp) = line
                    .trim()
                    .split_once(';')
                    .map(|(city, temp)| {
                        (city, temp.parse::<f32>().expect("Should parse correctly"))
                    })
                    .expect("There should be values");

                let values = measurements_map.entry(name.to_string()).or_insert((
                    f32::MAX,
                    f32::MIN,
                    0.0,
                    0,
                ));

                values.2 += temp;
                values.3 += 1;

                values.0 = values.0.min(temp);
                values.1 = values.1.max(temp);
                line.clear();
            }

            println!("\t Finished thread # {i}");
            (measurements_map, bilion)
        });
        handles.push(handle);
    }

    println!("Waiting for each thread");

    let acc = handles.into_iter().map(|x| x.join().unwrap()).fold(
        BTreeMap::new(),
        |mut acc, (element, _bil)| {
            for (k, v) in element {
                let values = acc.entry(k).or_insert((f32::MAX, f32::MIN, 0.0, 0));
                values.2 += v.2;
                values.3 += v.3;
                values.0 = values.0.min(v.0);
                values.1 = values.1.max(v.1);
            }
            acc
        },
    );

    /*
    let mut acc = BTreeMap::new();
    for handle in handles {
        let map = handle.join().unwrap();
        for (k, v) in map {
            let values = acc.entry(k).or_insert((f32::MAX, f32::MIN, 0.0, 0));
            values.2 += v.2;
            values.3 += 1;
            values.0 = values.0.min(v.0);
            values.1 = values.1.max(v.1)
        }
    }
    */
    let v = acc
        .iter()
        .map(|(k, v)| {
            let avg = v.2 / (v.3 as f32);
            format!("{k}={}/{}/{} total {} count {}", v.0, avg, v.1, v.2, v.3)
        })
        .collect::<Vec<_>>();

    print!("{{");
    for city in v.iter() {
        println!("{}", city);
    }
    println!("}}");
    println!("Final Size {}", v.len());

    /* Single Threaded
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut measurements_map = BTreeMap::<String, (f32, f32, f32, i64)>::new();

        while reader.read_line(&mut line).expect("Should not Fail") != 0 {
            let (name, temp) = line
                .trim()
                .split_once(';')
                .map(|(city, temp)| (city, temp.parse::<f32>().expect("Should parse correctly")))
                .expect("There should be values");

            let values =
                measurements_map
                    .entry(name.to_string())
                    .or_insert((f32::MAX, f32::MIN, 0.0, 0));

            values.2 += temp;
            values.3 += 1;

            values.0 = values.0.min(temp);
            values.1 = values.1.max(temp);
            line.clear();
        }

        let v = measurements_map
            .iter()
            .map(|(k, v)| {
                let avg = v.2 / (v.3 as f32);
                format!("{k}={}/{}/{}", v.0, avg, v.1)
            })
            .collect::<Vec<_>>();

        print!("{{");
        for city in v.iter() {
            println!("{}", city);
        }
        print!("}}");
    */

    // Calculate final output
}
