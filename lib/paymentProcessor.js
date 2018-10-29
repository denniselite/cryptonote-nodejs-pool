/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Payments processor
 **/

// Load required modules
var fs = require('fs');
var async = require('async');

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
var notifications = require('./notifications.js');
var utils = require('./utils.js');

// Initialize log system
var logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);

/**
 * Run payments processor
 **/

log('info', logSystem, 'Started');

if (!config.poolServer.paymentId) config.poolServer.paymentId = {};
if (!config.poolServer.paymentId.addressSeparator) config.poolServer.paymentId.addressSeparator = "+";
if (!config.payments.priority) config.payments.priority = 0;

function asynchronous_redis_get(key)
{
return new Promise(function(result, resulterror)
{
redisClient.get(key, function(error, data){
{
if (error) {
resulterror(0);
}
result(data);
});
});
}
 
function get_hard_fork_version()
{
return new Promise(function(result, resulterror)
{
apiInterfaces.rpcDaemon('hard_fork_info', "",function(error, data)
{
if (error) {
resulterror(1);
}
result(data.version);
});
});
}

function runInterval(){
    async.waterfall([

        // Get worker keys
        function(callback){
            redisClient.keys(config.coin + ':workers:*', function(error, result) {
                if (error) {
                    log('error', logSystem, 'Error trying to get worker balances from redis %j', [error]);
                    callback(true);
                    return;
                }
                callback(null, result);
            });
        },

        // Get worker balances
        function(keys, callback){
            var redisCommands = keys.map(function(k){
                return ['hget', k, 'balance'];
            });
            redisClient.multi(redisCommands).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Error with getting balances from redis %j', [error]);
                    callback(true);
                    return;
                }

                var balances = {};
                for (var i = 0; i < replies.length; i++){
                    var parts = keys[i].split(':');
                    var workerId = parts[parts.length - 1];

                    balances[workerId] = parseInt(replies[i]) || 0;
                }
                callback(null, keys, balances);
            });
        },

        // Get worker minimum payout
        function(keys, balances, callback){
            var redisCommands = keys.map(function(k){
                return ['hget', k, 'minPayoutLevel'];
            });
            redisClient.multi(redisCommands).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Error with getting minimum payout from redis %j', [error]);
                    callback(true);
                    return;
                }

                var minPayoutLevel = {};
                for (var i = 0; i < replies.length; i++){
                    var parts = keys[i].split(':');
                    var workerId = parts[parts.length - 1];

                    var minLevel = config.payments.minPayment;
                    var maxLevel = config.payments.maxPayment;
                    var defaultLevel = minLevel;

                    var payoutLevel = parseInt(replies[i]) || minLevel;
                    if (payoutLevel < minLevel) payoutLevel = minLevel;
                    if (maxLevel && payoutLevel > maxLevel) payoutLevel = maxLevel;
                    minPayoutLevel[workerId] = payoutLevel;

                    if (payoutLevel !== defaultLevel) {
                        log('info', logSystem, 'Using payout level of %s for %s (default: %s)', [ utils.getReadableCoins(minPayoutLevel[workerId]), workerId, utils.getReadableCoins(defaultLevel) ]);
                    }
                }
                callback(null, balances, minPayoutLevel);
            });
        },

        // Filter workers under balance threshold for payment
        async function(balances, minPayoutLevel, callback){
            var payments = {};

            for (var worker in balances){
                var balance = balances[worker];
                if (balance >= minPayoutLevel[worker]){
                    var remainder = balance % config.payments.denomination;
                    var payout = balance - remainder;

                    if (config.payments.dynamicTransferFee && config.payments.minerPayFee){
                        payout -= config.payments.transferFee;
                    }
                    if (payout < 0) continue;

                    payments[worker] = payout;
                }
            }

            if (Object.keys(payments).length === 0){
                log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
                callback(true);
                return;
            }

            var transferCommands = [];
            var addresses = 0;
            var commandAmount = 0;
            var commandIndex = 0;

            for (var worker in payments){
                var amount = parseInt(payments[worker]);
                if(config.payments.maxTransactionAmount && amount + commandAmount > config.payments.maxTransactionAmount) {
                    amount = config.payments.maxTransactionAmount - commandAmount;
                }

                var address = worker;
                var payment_id = null;

                var with_payment_id = false;

                var addr = address.split(config.poolServer.paymentId.addressSeparator);
                if ((addr.length === 1 && utils.isIntegratedAddress(address)) || addr.length >= 2){
                    with_payment_id = true;
                    if (addr.length >= 2){
                        address = addr[0];
                        payment_id = addr[1];
                        payment_id = payment_id.replace(/[^A-Za-z0-9]/g,'');
                        if (payment_id.length !== 16 && payment_id.length !== 64) {
                            with_payment_id = false;
                            payment_id = null;
                        }
                    }
                    if (addresses > 0){
                        commandIndex++;
                        addresses = 0;
                        commandAmount = 0;
                    }
                }

                if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
                    var addr = address.split(config.poolServer.fixedDiff.addressSeparator);
                    if (addr.length >= 2) address = addr[0];
                }

                var hard_fork_version = await get_hard_fork_version();
                if (hard_fork_version >= 10)
                {
                  var publictransactionsettings = config.payments.tx_privacy_settings;
                  if (publictransactionsettings === "settings")
                  {
                    var addresssettings = await asynchronous_redis_get(config.coin + ":publictransactionsettings:" + address);
                    publictransactionsettings = addresssettings == 1 ? "public" : "private";
                  }
                }

                if (hard_fork_version >= 10)
                {
                  if(!transferCommands[commandIndex]) {
                    transferCommands[commandIndex] = {
                        redis: [],
                        amount : 0,
                        rpc: {
                            destinations: [],
                            tx_privacy_settings: publictransactionsettings,
                            fee: config.payments.transferFee,
                            mixin: config.payments.mixin,
                            priority: config.payments.priority,
                            get_tx_keys: config.payments.get_tx_keys,
                            unlock_time: 0
                        }
                     };
                  }
                }
                else
                {
                  if(!transferCommands[commandIndex]) {
                    transferCommands[commandIndex] = {
                        redis: [],
                        amount : 0,
                        rpc: {
                            destinations: [],
                            fee: config.payments.transferFee,
                            mixin: config.payments.mixin,
                            priority: config.payments.priority,
                            get_tx_keys: config.payments.get_tx_keys,
                            unlock_time: 0
                        }
                     };
                  }
                }


                transferCommands[commandIndex].rpc.destinations.push({amount: amount, address: address});
                if (payment_id) transferCommands[commandIndex].rpc.payment_id = payment_id;

                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
                if(config.payments.dynamicTransferFee && config.payments.minerPayFee){
                    transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -config.payments.transferFee]);
                }
                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
                transferCommands[commandIndex].amount += amount;

                addresses++;
                commandAmount += amount;

                if (config.payments.dynamicTransferFee){
                    transferCommands[commandIndex].rpc.fee = config.payments.transferFee * addresses;
                }

                if (addresses >= config.payments.maxAddresses || (config.payments.maxTransactionAmount && commandAmount >= config.payments.maxTransactionAmount) || with_payment_id) {
                    commandIndex++;
                    addresses = 0;
                    commandAmount = 0;
                }
            }

            var timeOffset = 0;
            var notify_miners = [];

            var daemonType = config.daemonType ? config.daemonType.toLowerCase() : "default";

            async.filter(transferCommands, function(transferCmd, cback){
                var rpcCommand = "transfer_split";
                var rpcRequest = transferCmd.rpc;

                if (daemonType === "bytecoin") {
                    rpcCommand = "sendTransaction";
                    rpcRequest = {
                        transfers: transferCmd.rpc.destinations,
                        fee: transferCmd.rpc.fee,
                        anonymity: transferCmd.rpc.mixin,
                        unlockTime: transferCmd.rpc.unlock_time
                    };
                    if (transferCmd.rpc.payment_id) {
                        rpcRequest.paymentId = transferCmd.rpc.payment_id;
                    }
                }

                apiInterfaces.rpcWallet(rpcCommand, rpcRequest, function(error, result){
                    if (error){
                        log('error', logSystem, 'Error with %s RPC request to wallet daemon %j', [rpcCommand, error]);
                        log('error', logSystem, 'Payments failed to send to %j', transferCmd.rpc.destinations);
                        cback(false);
                        return;
                    }

                    var txhasharray = result.tx_hash_list;
                    var txamountarray = result.amount_list;
                    var txkeyarray = result.tx_key_list;
                    var counter;
                    for (counter = 0; counter < Object.keys(txhasharray).length; counter++)
                    {

                    // write the tx key to the log file
                    if (config.payments.get_tx_keys === true)
                    {
                    var str = "tx_hash:" + txhasharray[counter] + ", tx_key:" + txkeyarray[counter] + "\n";
                    fs.appendFile(config.customlogfiles.transactionDetails, str, function (error) {
                       if (error) {
                           log('error', logSystem, 'Error writing payments_txkey.log');
                       }
                    });
                    }

                    var now = (timeOffset++) + Date.now() / 1000 | 0;

                    transferCmd.redis.push(['zadd', config.coin + ':payments:all', now, [
                        txhasharray[counter],
                        txamountarray[counter],
                        transferCmd.rpc.fee,
                        transferCmd.rpc.mixin,
                        Object.keys(transferCmd.rpc.destinations).length
                    ].join(':')]);

                    var notify_miners_on_success = [];
                    for (var i = 0; i < transferCmd.rpc.destinations.length; i++){
                        var destination = transferCmd.rpc.destinations[i];
                        if (transferCmd.rpc.payment_id){
                            destination.address += config.poolServer.paymentId.addressSeparator + transferCmd.rpc.payment_id;
                        }
                        transferCmd.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                            txhasharray[counter],
                            txamountarray[counter],
                            transferCmd.rpc.fee,
                            transferCmd.rpc.mixin
                        ].join(':')]);

                        notify_miners_on_success.push(destination);
                    }
                  }

                    log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
                    redisClient.multi(transferCmd.redis).exec(function(error, replies){
                        if (error){
                            log('error', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
                            log('error', logSystem, 'Double payments likely to be sent to %j', transferCmd.rpc.destinations);
                            cback(false);
                            return;
                        }

                        for (var m in notify_miners_on_success) {
                            notify_miners.push(notify_miners_on_success[m]);
                        }

                        cback(true);
                    });
                });
            }, function(succeeded){
                var failedAmount = transferCommands.length - succeeded.length;

                for (var m in notify_miners) {
                    var notify = notify_miners[m];
                    log('info', logSystem, 'Payment of %s to %s', [ utils.getReadableCoins(notify.amount), notify.address ]);
                    notifications.sendToMiner(notify.address, 'payment', {
                        'ADDRESS': notify.address.substring(0,7)+'...'+notify.address.substring(notify.address.length-7),
                        'AMOUNT': utils.getReadableCoins(notify.amount),
                    });
                }
                log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);

                callback(null);
            });

        }

    ], function(error, result){
        setTimeout(runInterval, config.payments.interval * 1000);
    });
}

runInterval();
