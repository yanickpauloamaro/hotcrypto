use anyhow::{Result, bail};
use std::collections::BTreeMap;
use std::path::PathBuf;

use move_binary_format::{
    file_format::CompiledModule
};
use move_bytecode_verifier::{verify_module};
use move_compiler::{
    Compiler as MoveCompiler,
    compiled_unit::AnnotatedCompiledUnit,
    shared::NumericalAddress,
};
use move_ir_compiler::Compiler as IRCompiler;

pub struct Compiler {
    deps: Vec<CompiledModule>
}

impl Compiler {

    pub fn new() -> Result<Self> {

        let deps = compile_dependencies()?;

        for module in &deps {
            verify_module(&module).expect("Module must verify");
        }

        Ok(Self{deps})
    }

    fn compiler(&self) -> IRCompiler {
        IRCompiler::new(self.deps.iter().collect())
    }

    pub fn into_script_blob(self, code: &str) -> Result<Vec<u8>> {
        self.compiler().into_script_blob(code)
    }

    pub fn dependencies(&self) -> Vec<&CompiledModule> {
        self.compiler().deps
    }
}

fn compile_dependencies() -> Result<Vec<CompiledModule>> {
    expect_modules(compile_currency()?).collect()
}

fn expect_modules(
    units: impl IntoIterator<Item = AnnotatedCompiledUnit>,
) -> impl Iterator<Item = Result<CompiledModule>> {
    units.into_iter().map(|unit| match unit {
        AnnotatedCompiledUnit::Module(annot_module) => Ok(annot_module.named_module.module),
        AnnotatedCompiledUnit::Script(_) => bail!("expected modules got script"),
    })
}

fn compile_currency() -> Result<Vec<AnnotatedCompiledUnit>> {

    let move_files = vec![
        signer_file(), error_file(),
        currency_module_file()
    ];

    let move_compiler = MoveCompiler::new(
        vec![(move_files, currency_named_addresses())],
        vec![],
    );

    let (_, units) = move_compiler.build_and_report()
        .expect("Error compiling...");

    Ok(units)
}

pub fn currency_named_addresses() -> BTreeMap<String, NumericalAddress> {
    let pairs = [("Currency", "0xCAFE")];

    let mut mapping: BTreeMap<String, NumericalAddress> = pairs
        .iter()
        .map(|(name, addr)| (name.to_string(), NumericalAddress::parse_str(addr).unwrap()))
        .collect();

    let mut std_mapping = move_stdlib::move_stdlib_named_addresses();
    mapping.append(&mut std_mapping);

    mapping
}

pub fn currency_module_file() -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src/currency.move".to_string());
    path.to_str().unwrap().to_string()
}

fn signer_file() -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.push("move-main/language/move-stdlib/sources/Signer.move");
    path.to_str().unwrap().to_string()
}

fn error_file() -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.push("move-main/language/move-stdlib/sources/Errors.move");
    path.to_str().unwrap().to_string()
}
