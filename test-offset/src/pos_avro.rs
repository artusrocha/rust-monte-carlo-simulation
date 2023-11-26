#[derive(
    Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize, derive_builder::Builder,
)]
#[builder(setter(into))]
pub struct PosAvro {
    pub dst: i32,
    pub acc_id: i32,
    pub ins_id: i32,
    pub grp: i32,
    #[serde(with = "serde_bytes")]
    pub grpv: Vec<u8>,
    pub qty: f32,
    pub factor: f32,
    pub ratio: f32,
}
