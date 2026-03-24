use std::collections::HashMap;
use std::error::Error;
use std::fs::{read_dir, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use rayon::prelude::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};

fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let data_dir = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/data"));

    // Lectura del directorio. Si esto falla, es un error "global" => devolver Err.
    let paths: Vec<PathBuf> = read_dir(&data_dir)?
        .filter_map(|entry_res| match entry_res {
            Ok(entry) => Some(entry.path()),
            Err(e) => {
                eprintln!("Error leyendo una entrada del directorio {:?}: {}", data_dir, e);
                None
            }
        })
        .collect();

    // Procesamiento en paralelo. Si un archivo falla al abrirse, se loggea el error y se saltea el archivo.
    let result: HashMap<String, usize> = paths
        .par_iter()
        .flat_map(|path| {
            match File::open(path) {
                Ok(file) => {
                    // lines() devuelve Result<String, io::Error> por línea.
                    // par_bridge() paraleliza el iterador de líneas.
                    let reader = BufReader::new(file);
                    reader.lines().par_bridge()
                }
                Err(e) => {
                    eprintln!("No se pudo abrir el archivo {:?}: {}", path, e);
                    // Devolver un iterador vacío del mismo tipo (Result<String, io::Error>)
                    std::iter::empty().par_bridge()
                }
            }
        })
        // Manejo de error por línea: loggear y devolver None para saltear esa línea
        .filter_map(|line_res| match line_res {
            Ok(line) => Some(line),
            Err(e) => {
                eprintln!("Error leyendo una línea: {}", e);
                None
            }
        })
        // Wordcount por línea (local)
        .map(|line| {
            let mut counts: HashMap<String, usize> = HashMap::new();
            for w in line.split_whitespace() {
                *counts.entry(w.to_string()).or_insert(0) += 1;
            }

            counts
        })
        // Reducción: combinación de los HashMaps
        .reduce(HashMap::new, |mut acc, words| {
            for (k, v) in words {
                *acc.entry(k).or_insert(0) += v;
            }
            acc
        });

    println!("Tiempo total: {:?}", start.elapsed());
    println!("Cantidad de palabras distintas: {}", result.len());
    // Imprimir todo el hashmap en datasets enormes puede ser inmanejable:
    // println!("{:?}", result);

    Ok(())
}