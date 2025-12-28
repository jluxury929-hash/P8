const { ethers, Wallet, WebSocketProvider, JsonRpcProvider, Contract, Interface } = require('ethers');
require('dotenv').config();

// 1. BOOTSTRAP: MAXIMIZED INFRASTRUCTURE
console.log("-----------------------------------------");
console.log("🟢 [BOOT] PINNACLE WHALE HUNTER INITIALIZING...");

// AUTO-CONVERT WSS TO HTTPS FOR EXECUTION (Premium Stability)
const RAW_WSS = process.env.WSS_URL || "";
const EXECUTION_URL = RAW_WSS.replace("wss://", "https://");

const CONFIG = {
    CHAIN_ID: 8453,
    TARGET_CONTRACT: "0x83EF5c401fAa5B9674BAfAcFb089b30bAc67C9A0",
    
    // ⚡ DUAL-LANE INFRASTRUCTURE
    WSS_URL: RAW_WSS,          // Listener (Low Latency)
    RPC_URL: EXECUTION_URL,    // Executor (High Reliability)
    
    // 🏦 ASSETS
    WETH: "0x4200000000000000000000000000000000000006",
    USDC: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    
    // 🔮 ORACLES (The "Intelligence")
    GAS_ORACLE: "0x420000000000000000000000000000000000000F", // Base L1 Fee
    CHAINLINK_FEED: "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70", // ETH Price
    
    // 🐋 STRATEGY SETTINGS
    WHALE_THRESHOLD: ethers.parseEther("10"), // 10 ETH+ Trigger
    MIN_NET_PROFIT: "0.01", // ~$33 net profit floor
    
    // ⚙️ PERFORMANCE
    GAS_LIMIT: 1100000n, 
    PRIORITY_BRIBE: 15n, // 15% Tip to be FIRST
};

// Global State
let currentEthPrice = 0;
let nextNonce = 0;

async function startPinnacle() {
    // A. KEY SANITIZER (Prevents Crashes)
    let rawKey = process.env.TREASURY_PRIVATE_KEY;
    if (!rawKey) { console.error("❌ FATAL: Private Key missing."); process.exit(1); }
    const cleanKey = rawKey.trim();

    try {
        // B. DUAL-PROVIDER SETUP
        const httpProvider = new JsonRpcProvider(CONFIG.RPC_URL);
        const wsProvider = new WebSocketProvider(CONFIG.WSS_URL);
        const signer = new Wallet(cleanKey, httpProvider); // Signer uses HTTP (Stable)
        
        await wsProvider.ready;
        console.log(`✅ PINNACLE ONLINE | EXECUTOR: ${CONFIG.RPC_URL.substring(0, 25)}...`);

        // C. CONTRACTS
        const oracleContract = new Contract(CONFIG.GAS_ORACLE, ["function getL1Fee(bytes memory _data) public view returns (uint256)"], httpProvider);
        const priceFeed = new Contract(CONFIG.CHAINLINK_FEED, ["function latestRoundData() view returns (uint80,int256,uint256,uint256,uint80)"], httpProvider);
        const titanIface = new Interface(["function requestTitanLoan(address,uint256,address[])"]);

        // Sync Nonce
        nextNonce = await httpProvider.getTransactionCount(signer.address);

        // D. LIVE INTELLIGENCE LOOP
        wsProvider.on("block", async (blockNum) => {
            try {
                // 1. Update Price
                const [, price] = await priceFeed.latestRoundData();
                currentEthPrice = Number(price) / 1e8;
                process.stdout.write(`\r🌊 BLOCK: ${blockNum} | ETH: $${currentEthPrice.toFixed(2)} | Hunting... `);

                // 2. FETCH BLOCK TRANSACTIONS (Via HTTP)
                const block = await httpProvider.getBlock(blockNum, true);
                if (!block || !block.transactions) return;

                // 3. WHALE SCANNER
                // We scan every tx in the block. If any TX moves > 10 ETH, we strike.
                const whaleMove = block.transactions.find(t => t.value >= CONFIG.WHALE_THRESHOLD);

                if (whaleMove) {
                    console.log(`\n🚨 WHALE DETECTED: ${ethers.formatEther(whaleMove.value)} ETH | Hash: ${whaleMove.hash.slice(0, 10)}...`);
                    
                    // Trigger the Strike Logic
                    await executePinnacleStrike(httpProvider, signer, titanIface, oracleContract);
                }
            } catch (e) { /* Ignore block fetch errors */ }
        });

        // E. IMMORTALITY PROTOCOL
        wsProvider.websocket.onclose = () => {
            console.warn("\n⚠️ CONNECTION LOST. REBOOTING...");
            process.exit(1); 
        };

    } catch (e) {
        console.error(`\n❌ CRITICAL: ${e.message}`);
        setTimeout(startPinnacle, 1000);
    }
}

async function executePinnacleStrike(provider, signer, iface, oracle) {
    try {
        // 1. DYNAMIC LOAN (Using Real-Time Price)
        const balanceWei = await provider.getBalance(signer.address);
        const balanceEth = parseFloat(ethers.formatEther(balanceWei));
        const usdValue = balanceEth * currentEthPrice; 

        // Scale loan based on wealth to maximize leverage
        let loanAmount = ethers.parseEther("10");
        if (usdValue >= 200) loanAmount = ethers.parseEther("100");
        else if (usdValue >= 100) loanAmount = ethers.parseEther("75");
        else if (usdValue >= 50) loanAmount = ethers.parseEther("25");

        const path = [CONFIG.WETH, CONFIG.USDC];

        // 2. ENCODE DATA
        const strikeData = iface.encodeFunctionData("requestTitanLoan", [
            CONFIG.WETH, loanAmount, path
        ]);

        // 3. PRE-FLIGHT (Static Call + L1 Fee + Gas Data)
        const [simulation, l1Fee, feeData] = await Promise.all([
            provider.call({ to: CONFIG.TARGET_CONTRACT, data: strikeData, from: signer.address }).catch(() => null),
            oracle.getL1Fee(strikeData),
            provider.getFeeData()
        ]);

        if (!simulation) return; // Reverted

        // 4. MAXIMIZED COST CALCULATION
        // Aave V3 Fee: 0.05%
        const aaveFee = (loanAmount * 5n) / 10000n;
        // Priority Bribe: 15%
        const aggressivePriority = (feeData.maxPriorityFeePerGas * (100n + CONFIG.PRIORITY_BRIBE)) / 100n;
        
        const l2Cost = CONFIG.GAS_LIMIT * feeData.maxFeePerGas;
        const totalCost = l2Cost + l1Fee + aaveFee;
        
        const netProfit = BigInt(simulation) - totalCost;

        // 5. EXECUTION
        if (netProfit > ethers.parseEther(CONFIG.MIN_NET_PROFIT)) {
            const profitUSD = parseFloat(ethers.formatEther(netProfit)) * currentEthPrice;
            console.log(`💎 PROFIT CONFIRMED: ${ethers.formatEther(netProfit)} ETH (~$${profitUSD.toFixed(2)})`);
            
            const tx = await signer.sendTransaction({
                to: CONFIG.TARGET_CONTRACT,
                data: strikeData,
                gasLimit: CONFIG.GAS_LIMIT,
                maxFeePerGas: feeData.maxFeePerGas,
                maxPriorityFeePerGas: aggressivePriority, // Bribe
                nonce: nextNonce++,
                type: 2
            });
            
            console.log(`🚀 PINNACLE STRIKE: ${tx.hash}`);
            await tx.wait();
        }
    } catch (e) {
        if (e.message.includes("nonce")) nextNonce = await provider.getTransactionCount(signer.address);
    }
}

// EXECUTE
if (require.main === module) {
    startPinnacle().catch(e => {
        console.error("FATAL ERROR. RESTARTING...");
        setTimeout(startPinnacle, 1000);
    });
}
