//! A nagios plugin for checking PostgreSQL queries written in Rust
//!
//! ### Usage
//! ```sh
//! check_postgresql [OPTIONS] --db-connection-sting <user[:password]@host[:port][/database]> --query <QUERY>
//! ```
//! `check_postgresql` will connect to the given database, execute the query and compare (>=) the
//! result to the warning values (default: 1) and the critical values (default:2). If a list is given, both
//! warning and critical need to have the same length as the resultset.
//! It currently only supports integer types in the resultset.
//! `check_postgresql` will automatically convert Postgres' types "char", smallint, integer, bigint and oid to rust's i64.
//!
//! # Panics
//! The program will panic iff a wrong type (other than specified above) is queried.

extern crate clap;
extern crate postgres;
extern crate byteorder;
use postgres::{Connection, SslMode};
use std::str::FromStr;
use std::error::Error;
use postgres::types;
use postgres::types::{SessionInfo,Type};
use byteorder::{BigEndian,ReadBytesExt};
use std::io::prelude::Read;



// We need a new type which accepts all of postgres' integer types (we do not want to care about postgres type conversions)
struct Int64(i64);
impl Int64 {
    fn to_i64 (&self) -> i64 {
        let Int64(i) = *self;
        i
    }
}
impl types::FromSql for Int64 {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _: &SessionInfo) -> Result<Int64,postgres::error::Error> {
        let val = match ty {
            &Type::Char => try!(raw.read_i8()) as i64,
            &Type::Int2 => try!(raw.read_i16::<BigEndian>()) as i64,
            &Type::Int4 => try!(raw.read_i32::<BigEndian>()) as i64,
            &Type::Int8 => try!(raw.read_i64::<BigEndian>()) as i64,
            &Type::Oid => try!(raw.read_u32::<BigEndian>()) as i64,
            _ => try!(raw.read_i64::<BigEndian>()) as i64,
        };
        Ok(Int64(val))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Char | Type::Int2 | Type::Int4 | Type::Int8 | Type::Oid => true,
            _ => false
        }
    }
}


// The Status defines values needed for Nagios' plugin specification
enum StatusType {
    OK,
    WARNING,
    CRITICAL,
    UNKNOWN,
}
struct Status {
    t : StatusType,
    description : String,
}
impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let _ = match self.t {
            StatusType::OK => write!(f, "OK|{}", self.description),
            StatusType::WARNING => write!(f, "WARNING|{}", self.description ),
            StatusType::CRITICAL => write!(f, "CRITICAL|{}", self.description ),
            StatusType::UNKNOWN => write!(f, "UNKNOWN|{}", self.description ),
        };
        Ok(())
    }
}

// Small helper function for returning a Nagios status
fn exit_nagios (status : Status ) {
    let return_value : i32 = match status.t {
        StatusType::OK => 0,
        StatusType::WARNING => 1,
        StatusType::CRITICAL => 2,
        StatusType::UNKNOWN => 3,
    };

    print!("{}",status.to_string());
    std::process::exit(return_value);
}

fn main() {

    // Argument parsing
    let matches = clap::App::new("check_postgresql")
        .version("0.1.0")
        .author("Jens Heyens")
        .arg(clap::Arg::with_name("conn")
            .short("d")
            .long("db-connection-string")
            .value_name("user[:password]@host[:port][/database]")
            .help("The connection String ")
            .takes_value(true)
            .required(true))
        .arg(clap::Arg::with_name("query")
            .short("q")
            .long("query")
            .value_name("QUERY")
            .help("The PG query to execute")
            .takes_value(true)
            .required(true))
        .arg(clap::Arg::with_name("warn")
            .short("w")
            .long("warn")
            .value_name("n1[,n2...]")
            .help("defines warning result")
            .takes_value(true)
            .required(false))
        .arg(clap::Arg::with_name("crit")
            .short("c")
            .long("critical")
            .value_name("n1[,n2...]")
            .help("defines critical result")
            .takes_value(true)
            .required(false))
        .get_matches();

    let warn_string = matches.value_of("warn");
    let crit_string = matches.value_of("crit");

    let mut vec_warn : Vec<i64> = vec![];
    let mut vec_crit : Vec<i64> = vec![];

    if let Some(str) = warn_string {
        for i in str.to_string().split(','){vec_warn.push(match i64::from_str(i) {Ok(i) => i, Err(t) => panic!(t)})};
    } else {
        vec_warn.push(1);
    }

    if let Some(str) = crit_string {
        for i in str.to_string().split(',') {vec_crit.push(match i64::from_str(i) {Ok(i) => i, Err(t) => panic!(t)})};
    } else {
        vec_crit.push(2);
    }

    // Make sure we do not have different sized warning and critical vectors
    if vec_warn.len()!=vec_crit.len() {exit_nagios(Status{t : StatusType::UNKNOWN, description : "Size of integer arrays need to match".to_string()})
    };


    // Should not panic, since argument parsing should prevent empty strings
    let query_string = match matches.value_of("query") {
        Some(str) => str,
        None => panic!("No query provided!")
    };
    let connection_string = match matches.value_of("conn") {
        Some(str) => str,
        None => panic!("No connection string provided!")
    };


    // Connect to the database and execute the query. This cannot panic in unwrap, since Pattern matching exits program via `exit_nagios` on errors.
    let url : &str = &("postgresql://".to_string() + connection_string);
    let conn = match Connection::connect(url, SslMode::None) {
        Ok(conn) => Ok(conn),
        Err(err) => {
            exit_nagios(Status{t : StatusType::UNKNOWN, description: err.description().to_string()});
            Err(err)
            }
    }.unwrap();
    let rows = match conn.query(query_string, &[]) {
        Ok(rows) => Ok(rows),
        Err(err) => {
            exit_nagios(Status{t : StatusType::UNKNOWN, description: err.description().to_string()});
            Err(err)
            }
    }.unwrap() ;


    if rows.len()==0 {
        exit_nagios(Status{t : StatusType::UNKNOWN, description: "Query did return empty row set".to_string()})
    }
    for row in rows.iter() {
        if row.len() != vec_warn.len() {
            exit_nagios(Status{t : StatusType::UNKNOWN, description : "Size of result set and integer array need to match".to_string()})
        }
        let mut status = StatusType::OK;
        for i in 0..vec_warn.len() { // They should all have the same length by now.
            if vec_warn[i] <= row.get::<usize,Int64>(i).to_i64()  {status = StatusType::WARNING; break}
        }
        for i in 0..vec_crit.len() {
            if vec_crit[i] <= row.get::<usize,Int64>(i).to_i64()  {status = StatusType::CRITICAL; break}
        }

        // print result set as tuple `(s1,..,sn)`
        let mut description : String = "Result:(".to_string();
        for j in 0..row.len() {
            description = description + &(row.get::<usize,Int64>(j).to_i64().to_string());
            if j != row.len()-1 {
                description = description + &",";
            }
            description = description + &")";
        }
        exit_nagios(Status{t : status, description : description})
    };
}
