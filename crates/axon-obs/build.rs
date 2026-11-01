fn main() {
    // include_dir! reads ui/dist at compile time without telling cargo, so a rebuilt UI would not reach the binary.
    println!("cargo:rerun-if-changed=ui/dist");
}
