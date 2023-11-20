const fs = require("fs");
const path = require('node:path'); 

let i_batch_id = 1;
const default_time_limit = 7;

const item_limit = 20_000_000;
const t1_writer = getWriter("/tmp/t1.tsv")
const t2_writer = getWriter("/tmp/t2.tsv")

function getWriter (filename) {
    const writer = fs.createWriteStream(filename);
    writer.on('error',  (error) => console.log(`An error occurred while writing to the file. Error: ${error.message}`) )
    return writer;
}

function getRandomIntBetween(min, max) {
    return min+Math.floor(Math.random() * (max-min))
}

const ONE_PERCENT=Math.floor(item_limit/100)

function genItem(item_id) {

for(let i=0; i<ONE_PERCENT; i++) {
    if (item_id % ONE_PERCENT == 0) {
        console.log(Math.floor(100/(item_limit/item_id)), "%")
    }

    if (item_id > item_limit)
        return;

    t1_writer.write( [
        ...Object.values({
            acc_id: (1+Math.floor(Math.random() * 3_000)),
            par_id: 1,
            ins_id: item_id,
            cat: (1+Math.floor(Math.random() * 1_000)),
            qty: (1+Math.floor(Math.random() * 10_000_000)),
            factor: (1+Math.random()*4),        
        })
        //, ...gen_random_fields(10)
    ].join("\t").concat("\n") )


    t2_writer.write( [
        ...Object.values({
            acc_id: 1,
            par_id: 1,
            ins_id: item_id,
            cat: (1+Math.floor(Math.random() * 1_000)),
            qty: (1+Math.floor(Math.random() * 10_000_000)),
            factor: (1+Math.random()*4),        
        })
        //, ...gen_random_fields(10)
    ].join("\t").concat("\n") )

    item_id++

}

    setImmediate(() => genItem(item_id));
}

function gen_random_fields(n) {
    return [...Array(n).keys()].map( () => Math.floor(Math.random() * 10_000_000))
}

genItem(1)
