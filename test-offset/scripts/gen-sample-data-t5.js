const fs = require("fs");
const path = require('node:path'); 

const acc_writer = getWriter("/tmp/acc.tsv")
const t5_writer = getWriter("/tmp/t5.tsv")

const par_id = process.argv[2] ? Number(process.argv[2]) : 1
const item_limit = process.argv[3] ? Number(process.argv[3]) : 1000
const ONE_PERCENT=Math.floor(item_limit/100)

console.log(process.argv)
console.log({par_id, item_limit, ONE_PERCENT})

function getWriter (filename) {
    const writer = fs.createWriteStream(filename);
    writer.on('error',  (error) => console.log(`An error occured while writing to the file. Error: ${error.message}`) )
    return writer;
}


function gen_item(acc_id) {

    if (counter >= item_limit)
        return;

    const acc_ids = [acc_id, (acc_id + 10_000_000), (acc_id + 20_000_000), (acc_id + 30_000_000), (acc_id + 40_000_000) ]
    acc_ids.forEach((id) => {
        acc_writer.write(
            Object.values({
                id,
                par_id,
                dst: acc_id,
            }).join("\t").concat("\n")
        )
    })

    for(let ins_id=1; ins_id<=1000; ins_id++) {
        write_item(rand_arr(acc_ids), ins_id, ins_id, 'L', 1)
        write_item(rand_arr(acc_ids), ins_id+10_000_000, ins_id, 'L', 1)
        write_item(rand_arr(acc_ids), ins_id+20_000_000, ins_id, 'L', 1)
        
        write_item(rand_arr(acc_ids), ins_id+30_000_000, ins_id, 'L', -1)
        write_item(rand_arr(acc_ids), ins_id+40_000_000, ins_id, 'L', -1)
        write_item(rand_arr(acc_ids), ins_id+50_000_000, ins_id, 'L', -1)
    }

    if (counter > 0 && counter % ONE_PERCENT == 0) {
        console.log(Math.floor(100/(item_limit/counter)), "%")
    }

    setImmediate(() => gen_item(++acc_id) );
}

function write_item(acc_id, ins_id, grp, grpv, long_or_short) {

    if (counter >= item_limit)
        return;
    
    counter++

    t5_writer.write(
        Object.values({
            acc_id,
            ins_id,
            grp,
            grpv,
            qty: (1+Math.floor(Math.random() * 10_000_000)) * long_or_short,
            factor: (1+Math.random()*4),        
        }).join("\t").concat("\n")
    )
}

function rand_arr(arr) {
    return arr[Math.floor(Math.random()*arr.length)]
}

let counter = 0
gen_item((par_id*1_000_000))
