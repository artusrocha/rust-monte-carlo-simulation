const fs = require("fs");
const { randomUUID } = require("node:crypto");
const path = require('node:path'); 

let i_batch_id = 1;
const default_time_limit = 7;

const QTY_OF_PRODUCTS_LIMIT = 50;
if ( ! process.argv[2]) {
    console.log('missing date parameter. e.g:');
    const dir=path.posix.basename(path.dirname(process.argv[1]))
    console.log(`   ${path.posix.basename(process.argv[0])} ${dir}/${path.posix.basename(process.argv[1])} 2023-11-14`)
    process.exit(1)

}
console.log(`parsing date: ${process.argv[2]}`);
const date = new Date(process.argv[2])
console.log(`using date: ${date.toISOString()}`);

const product_writer = getWriter("sample/product_props.tsv")
const product_mov_hist_writer = getWriter("sample/product_mov_hist.tsv")
const product_batch_writer = getWriter("sample/product_batch.tsv")

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

function genProduct(count) {

    if (count > QTY_OF_PRODUCTS_LIMIT)
        return;

    let product_id = randomUUID();
    product_writer.write(
        [
            product_id,         //id UUID
            90,                 // simulation_forecast_days SMALLINT CHECK(default_simulation_forecast_days >= 0),
            0.02,               // scenario_random_range_factor DECIMAL(3,2),
            1825,               // maximum_historic_days SMALLINT CHECK(default_maximum_historic >= 0),
            15*30,              // maximum_quantity INTEGER CHECK(maximum_quantity >= 0) NOT NULL,
            0,                  // minimum_quantity INTEGER CHECK(minimum_quantity >= 0) DEFAULT 0 NOT NULL,
            getRandomIntBetween(10, 90), // new_batch_default_expiration_days SMALLINT
            true,               // active BOOLEAN NOT NULL DEFAULT TRUE,
            date.toISOString(), // created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            date.toISOString(), // updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        ].join("\t").concat("\n")
    )

    let hist_date = new Date(2017, 11, 9)
    while ( hist_date.isBefore(date) ) {
        hist_date.setDate(hist_date.getDate() + 1)
        product_mov_hist_writer.write( 
            [
                product_id,                   // product_id UUID REFERENCES product_props (id),
                getRandomIntBetween(20, 100), // entry_qty INTEGER NOT NULL DEFAULT 0,
                getRandomIntBetween(20, 100), // withdrawal_qty INTEGER NOT NULL DEFAULT 0,
                hist_date.toISOString(),      // mov_date DATE NOT NULL DEFAULT NOW(),
                hist_date.getWeekNumber(),    // week_of_year SMALLINT CHECK(week_of_year >= 1 AND week_of_year <= 53) ,
                hist_date.toISOString(),      // created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ].join("\t").concat("\n")
        )
    }

    for (let day=15; day>=1; day--) {
        const entry_date = new Date()
        entry_date.setDate(date.getDate() - day)
        const deadline_date = new Date()
        deadline_date.setDate(date.getDate() + default_time_limit)
        const entry_date_iso = entry_date.toISOString()
        const deadline_date_iso = deadline_date.toISOString()
        const quantity = getRandomIntBetween(10, 30)
        product_batch_writer.write(
            [
                i_batch_id++,      // id SERIAL,
                product_id,        // product_id UUID REFERENCES product_props (id),
                entry_date_iso,    // entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                deadline_date_iso, // deadline_date TIMESTAMPTZ NOT NULL,
                deadline_date > date ? deadline_date_iso : 'null', // finished_date TIMESTAMPTZ,
                quantity,          // quantity INTEGER NOT NULL CHECK (quantity >= 0) DEFAULT 0;
                entry_date_iso,    // created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                entry_date_iso,    // updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ]
            .join("\t").concat("\n"))
    }

    setImmediate(() => genProduct(++count));
}

genProduct(1)
