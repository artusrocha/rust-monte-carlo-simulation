use apache_avro::Schema;
use bigdecimal::ToPrimitive;

use crate::pg_repository;
use crate::pos_avro;

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};

const POS_AVRO_SCHEMA: &str = include_str!("../position.avsc");

pub(crate) struct AvroParser {
    schema: Schema,
}

impl AvroParser {
    pub fn new() -> AvroParser {
        let schema = apache_avro::Schema::parse_str(POS_AVRO_SCHEMA).unwrap();
        AvroParser { schema }
    }

    pub fn to_avro_buf(&self, pos: &pg_repository::Pos) -> Vec<u8> {
        let pos = pos_avro::PosAvroBuilder::default()
            .dst(pos.dst)
            .acc_id(pos.acc_id)
            .ins_id(pos.ins_id)
            .grp(pos.grp)
            .grpv(pos.grpv.to_owned())
            .qty(pos.qty.to_f32().unwrap())
            .factor(pos.factor.to_f32().unwrap())
            .ratio(pos.ratio.to_f32().unwrap())
            .build()
            .unwrap();

        to_avro_datum(&self.schema, to_value(&pos).unwrap()).unwrap()
    }

    pub fn from_avro_buf(&self, pos_avr_buf: &Vec<u8>) -> pos_avro::PosAvro {
        let p1 = from_avro_datum(&self.schema, &mut pos_avr_buf.as_slice(), None).unwrap();
        from_value::<pos_avro::PosAvro>(&p1).unwrap()
    }
}
