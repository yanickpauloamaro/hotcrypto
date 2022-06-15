script {
    use Currency::BasicCoin;

    fun main(account: signer, to: address, amount: u64) {
            BasicCoin::transfer(&account, to, amount);
            return
    }
}