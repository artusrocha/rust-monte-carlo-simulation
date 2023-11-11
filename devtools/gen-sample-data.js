const fs = require("fs");

let i_batch_id = 1;
const default_time_limit = 7;

const item_limit = 50000;
const date = new Date()

const item_writer = getWriter("sample/data/item.tsv")
const item_mov_hist_writer = getWriter("sample/data/item_mov_hist.tsv")
const item_batch_writer = getWriter("sample/data/item_batch.tsv")

function getWriter (filename) {
    const writer = fs.createWriteStream(filename);
    writer.on('error',  (error) => console.log(`An error occured while writing to the file. Error: ${error.message}`) )
    return writer;
}

Date.prototype.isBefore = function (dateB) {
    return new Date(this.toDateString()) < new Date(dateB.toDateString());
};

Date.prototype.getWeekNumber = function () {
    var d = new Date(Date.UTC(this.getFullYear(), this.getMonth(), this.getDate()));
    var dayNum = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);
    var yearStart = new Date(Date.UTC(d.getUTCFullYear(),0,1));
    return Math.ceil((((d - yearStart) / 86400000) + 1)/7)
};

function getRandomIntBetween(min, max) {
    return min+Math.floor(Math.random() * (max-min))
}

function genItem(item_id) {

    if (item_id > item_limit)
        return;

    item_writer.write( [item_id, default_time_limit, true, date.toISOString(), date.toISOString()].join("\t").concat("\n") )

//    item_mov_hist_writer.cork()
    let hist_date = new Date(2017, 11, 9)
    while ( hist_date.isBefore(date) ) {
        hist_date.setDate(hist_date.getDate() + 1)
        item_mov_hist_writer.write( 
            [item_id, getRandomIntBetween(20, 100), getRandomIntBetween(20, 100), hist_date.toISOString(), hist_date.getWeekNumber(), hist_date.toISOString()]
            .join("\t").concat("\n")
        )
    }
//    process.nextTick(() => item_mov_hist_writer.uncork());


    for (let day=15; day>=1; day--) {
        const entry_date = new Date()
        entry_date.setDate(date.getDate() - day)
        const deadline_date = new Date()
        deadline_date.setDate(entry_date.getDate() + default_time_limit)
        const entry_date_iso = entry_date.toISOString()
        const deadline_date_iso = deadline_date.toISOString()
        item_batch_writer.write([i_batch_id++, item_id, entry_date_iso, deadline_date_iso, 'null', entry_date_iso, entry_date_iso].join("\t").concat("\n"))
    }

    setImmediate(() => genItem(++item_id));
}

genItem(1)
