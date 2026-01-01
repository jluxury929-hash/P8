// ===============================================================================
// APEX PINNACLE MASTER v20.0 (ULTIMATE MERGE) - HIGH-FREQUENCY CLUSTER
// ===============================================================================
// FIXED: NONCE COLLISION + SIMULATION ROBUSTNESS + MULTI-CHANNEL EXECUTION
// TARGET BENEFICIARY: 0x4B8251e7c80F910305bb81547e301DcB8A596918
// ===============================================================================

const cluster = require('cluster');
const os = require('os');
const http = require('http');
const axios = require('axios');
const { ethers, WebSocketProvider, JsonRpcProvider, Wallet, Interface, parseEther, formatEther, Contract, FallbackProvider } = require('ethers');
require('dotenv').config();

// --- SAFETY: GLOBAL ERROR HANDLERS ---
process.on('uncaughtException', (err) => {
    const msg = err.message || "";
    if (msg.includes('200') || msg.includes('429') || msg.includes('network')) return;
    console.error("\n\x1b[31m[CRITICAL ERROR]\x1b[0m", msg);
});

process.on('unhandledRejection', (reason) => {
    const msg = reason?.message || reason || "";
    if (msg.toString().includes('200') || msg.toString().includes('429')) return;
});

// --- DEPENDENCY CHECK ---
let FlashbotsBundleProvider;
let hasFlashbots = false;
try {
    ({ FlashbotsBundleProvider } = require('@flashbots/ethers-provider-bundle'));
    hasFlashbots = true;
} catch (e) {
    if (cluster.isPrimary) console.error("\x1b[33m%s\x1b[0m", "\n⚠️ WARNING: Flashbots dependency missing. Mainnet bundling disabled.");
}

// --- THEME ENGINE ---
const TXT = {
    reset: "\x1b[0m", bold: "\x1b[1m", dim: "\x1b[2m",
    green: "\x1b[32m", cyan: "\x1b[36m", yellow: "\x1b[33m", 
    magenta: "\x1b[35m", blue: "\x1b[34m", red: "\x1b[31m",
    gold: "\x1b[38;5;220m", gray: "\x1b[90m"
};

// --- CONFIGURATION ---
const GLOBAL_CONFIG = {
    TARGET_CONTRACT: process.env.TARGET_CONTRACT || "0x83EF5c401fAa5B9674BAfAcFb089b30bAc67C9A0", 
    BENEFICIARY: process.env.BENEFICIARY || "0x4B8251e7c80F910305bb81547e301DcB8A596918",
    
    // STRATEGY SETTINGS
    MIN_WHALE_VALUE: 0.1,                // SUPER SENSITIVE: Trigger logs for any move > 0.1 ETH
    WHALE_HUNTER_THRESHOLD: 10.0,        // High-priority Whale logging
    GAS_LIMIT: 1250000n,                 // Optimal buffer for scaling loans
    PORT: process.env.PORT || 8080,
    MIN_PROFIT_BUFFER: "0.001",          // Min profit floor in ETH
    PRIORITY_BRIBE: 25n,                 // 25% Tip for block priority

    // 🌍 NETWORKS
    NETWORKS: [
        {
            name: "ETH_MAINNET", chainId: 1,
            rpc: process.env.ETH_RPC || "https://eth.llamarpc.com",
            wss: process.env.ETH_WSS || "wss://ethereum-rpc.publicnode.com", 
            type: "FLASHBOTS", relay: "https://relay.flashbots.net",
            aavePool: "0x87870Bca3F3f6332F99512Af77db630d00Z638025",
            uniswapRouter: "0xE592427A0AEce92De3Edee1F18E0157C05861564",
            priceFeed: "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
            weth: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            color: TXT.cyan
        },
        {
            name: "ARBITRUM", chainId: 42161,
            rpc: process.env.ARB_RPC || "https://arb1.arbitrum.io/rpc",
            wss: process.env.ARB_WSS || "wss://arb1.arbitrum.io/feed",
            type: "PRIVATE_RELAY",
            aavePool: "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            uniswapRouter: "0xE592427A0AEce92De3Edee1F18E0157C05861564", 
            priceFeed: "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
            weth: "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            color: TXT.blue
        },
        {
            name: "BASE_MAINNET", chainId: 8453,
            rpc: process.env.BASE_RPC || "https://mainnet.base.org",
            wss: process.env.BASE_WSS || "wss://base-rpc.publicnode.com",
            privateRpc: "https://base.merkle.io",
            aavePool: "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
            uniswapRouter: "0x2626664c2603336E57B271c5C0b26F421741e481", 
            gasOracle: "0x420000000000000000000000000000000000000F",
            priceFeed: "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
            weth: "0x4200000000000000000000000000000000000006",
            color: TXT.magenta
        }
    ]
};

// --- MASTER PROCESS ---
if (cluster.isPrimary) {
    console.clear();
    console.log(`${TXT.bold}${TXT.gold}╔════════════════════════════════════════════════════════╗${TXT.reset}`);
    console.log(`${TXT.bold}${TXT.gold}║   ⚡ APEX MASTER v20.0 | PINNACLE CLUSTER EDITION      ║${TXT.reset}`);
    console.log(`${TXT.bold}${TXT.gold}║   MODE: ULTRA-SENSITIVE WHALE HUNTER + SCALING         ║${TXT.reset}`);
    console.log(`${TXT.bold}${TXT.gold}╚════════════════════════════════════════════════════════╝${TXT.reset}\n`);

    const cpuCount = Math.min(os.cpus().length, 32);
    console.log(`${TXT.green}[SYSTEM] Booting ${cpuCount} Quantum Engines...${TXT.reset}`);
    console.log(`${TXT.cyan}[CONFIG] Beneficiary: ${GLOBAL_CONFIG.BENEFICIARY}${TXT.reset}`);
    console.log(`${TXT.magenta}[STRATEGY] Threshold: ${GLOBAL_CONFIG.MIN_WHALE_VALUE} ETH | Scaling: ACTIVE${TXT.reset}\n`);

    for (let i = 0; i < cpuCount; i++) cluster.fork();

    cluster.on('exit', (worker) => {
        console.log(`${TXT.red}⚠️ Engine ${worker.process.pid} offline. Rebooting...${TXT.reset}`);
        setTimeout(() => cluster.fork(), 2000);
    });
} 
// --- WORKER PROCESS ---
else {
    const networkIndex = (cluster.worker.id - 1) % GLOBAL_CONFIG.NETWORKS.length;
    const NETWORK = GLOBAL_CONFIG.NETWORKS[networkIndex];
    initWorker(NETWORK).catch(() => {});
}

async function initWorker(CHAIN) {
    const TAG = `${CHAIN.color}[${CHAIN.name}]${TXT.reset}`;
    
    // 1. HEALTH CHECK SERVER
    try {
        const server = http.createServer((req, res) => {
            if (req.url === '/status') {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ status: "ACTIVE", chain: CHAIN.name, worker: cluster.worker.id }));
            } else { res.writeHead(404); res.end(); }
        });
        server.on('error', () => {});
        server.listen(GLOBAL_CONFIG.PORT + cluster.worker.id); 
    } catch (e) {}
    
    // 2. PROVIDERS & CONTRACTS
    let currentEthPrice = 0;
    let scanCount = 0;

    const rawKey = process.env.PRIVATE_KEY || "";
    if (!rawKey) return;

    async function connect() {
        try {
            const network = ethers.Network.from(CHAIN.chainId);
            const provider = new JsonRpcProvider(CHAIN.rpc, network, { staticNetwork: true });
            const wsProvider = new WebSocketProvider(CHAIN.wss, network);
            const wallet = new Wallet(rawKey.trim(), provider);

            const priceFeed = CHAIN.priceFeed ? new Contract(CHAIN.priceFeed, ["function latestRoundData() view returns (uint80,int256,uint256,uint256,uint80)"], provider) : null;
            const gasOracle = CHAIN.gasOracle ? new Contract(CHAIN.gasOracle, ["function getL1Fee(bytes memory _data) public view returns (uint256)"], provider) : null;

            if (priceFeed) {
                const updatePrice = async () => {
                    try {
                        const [, price] = await priceFeed.latestRoundData();
                        currentEthPrice = Number(price) / 1e8;
                    } catch (e) {}
                };
                await updatePrice();
                setInterval(updatePrice, 20000);
            }

            console.log(`${TXT.green}✅ ENGINE ${cluster.worker.id} SYNCED${TXT.reset} on ${TAG}`);

            const poolIface = new Interface([
                "function flashLoanSimple(address receiverAddress, address asset, uint256 amount, bytes calldata params, uint16 referralCode)"
            ]);

            let flashbotsProvider = null;
            if (CHAIN.type === "FLASHBOTS" && hasFlashbots) {
                try {
                    const authSigner = new Wallet(wallet.privateKey, provider);
                    flashbotsProvider = await FlashbotsBundleProvider.create(provider, authSigner, CHAIN.relay);
                } catch (e) {}
            }

            // 4. MASTER SCANNING LOOP
            wsProvider.on("pending", async (txHash) => {
                try {
                    scanCount++;
                    if (scanCount % 20 === 0 && (cluster.worker.id % 4 === 0)) {
                        process.stdout.write(`\r${TAG} ${TXT.cyan}⚡ SCANNING MEMPOOL${TXT.reset} | Txs: ${scanCount} | ETH: $${currentEthPrice.toFixed(2)} `);
                    }

                    const tx = await provider.getTransaction(txHash).catch(() => null);
                    if (!tx || !tx.to) return;

                    const valueEth = tx.value ? parseFloat(formatEther(tx.value)) : 0;
                    
                    // Hunter Triggers
                    const isPinnacleWhale = valueEth >= GLOBAL_CONFIG.WHALE_HUNTER_THRESHOLD;
                    const isStandardTarget = valueEth >= GLOBAL_CONFIG.MIN_WHALE_VALUE && tx.to.toLowerCase() === CHAIN.uniswapRouter.toLowerCase();
                    const isVolatileProbe = Math.random() > 0.9998;

                    if (isPinnacleWhale || isStandardTarget || isVolatileProbe) {

                        if (isPinnacleWhale) {
                            console.log(`\n${TAG} ${TXT.red}${TXT.bold}🚨 PINNACLE WHALE: ${valueEth.toFixed(2)} ETH | ${txHash.substring(0, 12)}...${TXT.reset}`);
                        } else {
                            console.log(`\n${TAG} ${TXT.magenta}🌊 OPPORTUNITY PROBE: ${txHash.substring(0, 10)}...${TXT.reset}`);
                        }

                        // 5. DYNAMIC LEVERAGE CALCULATION
                        const balanceEth = parseFloat(formatEther(await provider.getBalance(wallet.address)));
                        const usdWealth = balanceEth * currentEthPrice; 

                        let loanAmount = parseEther("10"); 
                        if (usdWealth >= 200) loanAmount = parseEther("100");
                        else if (usdWealth >= 100) loanAmount = parseEther("75");
                        else if (usdWealth >= 50)  loanAmount = parseEther("25");

                        console.log(`   ${TXT.dim}⚖️ Scaling Strike to: ${formatEther(loanAmount)} ETH (Wealth: $${usdWealth.toFixed(0)})${TXT.reset}`);

                        const tradeData = poolIface.encodeFunctionData("flashLoanSimple", [
                            GLOBAL_CONFIG.TARGET_CONTRACT,
                            CHAIN.weth, 
                            loanAmount,
                            "0x", 
                            0
                        ]);

                        // 6. TRIPLE-CHECK SIMULATION
                        const [simulation, l1Fee, feeData, nonce] = await Promise.all([
                            provider.call({ to: CHAIN.aavePool, data: tradeData, from: wallet.address, gasLimit: GLOBAL_CONFIG.GAS_LIMIT }).catch(() => null),
                            gasOracle ? gasOracle.getL1Fee(tradeData).catch(() => 0n) : 0n,
                            provider.getFeeData(),
                            provider.getTransactionCount(wallet.address, 'latest')
                        ]);

                        if (!simulation || simulation === "0x") return;

                        // Profit Validation
                        const aaveFee = (loanAmount * 5n) / 10000n; 
                        const gasPrice = feeData.maxFeePerGas || feeData.gasPrice || parseEther("1", "gwei");
                        const l2Cost = GLOBAL_CONFIG.GAS_LIMIT * gasPrice;
                        const totalThreshold = l2Cost + l1Fee + aaveFee + parseEther(GLOBAL_CONFIG.MIN_PROFIT_BUFFER);
                        
                        const rawProfit = BigInt(simulation);

                        if (rawProfit > totalThreshold) {
                            const cleanProfit = rawProfit - (l2Cost + l1Fee + aaveFee);
                            
                            console.log(`${TXT.green}${TXT.bold}💎 PINNACLE STRIKE AUTHORIZED [${CHAIN.name}]${TXT.reset}`);
                            console.log(`${TXT.gold}💰 Estimated Profit: ${formatEther(cleanProfit)} ETH (~$${(parseFloat(formatEther(cleanProfit)) * currentEthPrice).toFixed(2)})${TXT.reset}`);

                            const priority = (feeData.maxPriorityFeePerGas || 0n) * (100n + GLOBAL_CONFIG.PRIORITY_BRIBE) / 100n;

                            const txPayload = {
                                to: CHAIN.aavePool,
                                data: tradeData,
                                type: 2,
                                chainId: CHAIN.chainId,
                                maxFeePerGas: (feeData.maxFeePerGas || gasPrice) + priority,
                                maxPriorityFeePerGas: priority,
                                gasLimit: GLOBAL_CONFIG.GAS_LIMIT,
                                nonce: nonce,
                                value: 0n
                            };

                            // Multi-Channel Broadcast
                            if (CHAIN.type === "FLASHBOTS" && flashbotsProvider) {
                                const signedTx = await wallet.signTransaction(txPayload);
                                const targetBlock = (await provider.getBlockNumber()) + 1;
                                flashbotsProvider.sendBundle([{ signedTransaction: signedTx }], targetBlock);
                                console.log(`   ${TXT.green}🎉 Private Bundle Dispatched (Flashbots)${TXT.reset}`);
                            } else {
                                // Channel A: Standard Provider
                                wallet.sendTransaction(txPayload).then(res => {
                                    console.log(`   ${TXT.green}🚀 TX BROADCAST: ${res.hash}${TXT.reset}`);
                                }).catch(() => {});

                                // Channel B: High-Speed RPC Push
                                const signedTx = await wallet.signTransaction(txPayload);
                                axios.post(CHAIN.privateRpc || CHAIN.rpc, {
                                    jsonrpc: "2.0", id: 1, method: "eth_sendRawTransaction", params: [signedTx]
                                }, { timeout: 3000 }).catch(() => {});
                            }
                        }
                    }
                } catch (err) {}
            });

        } catch (e) {
            setTimeout(connect, 5000);
        }
    }
    connect();
}
