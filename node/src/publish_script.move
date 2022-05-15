script {
    use Currency::BasicCoin;

    fun main(owner: signer, account: signer, account_addr: address, amount: u64) {
        BasicCoin::publish_balance(&account);
        BasicCoin::mint(&owner, account_addr, amount);
        return
    }
}