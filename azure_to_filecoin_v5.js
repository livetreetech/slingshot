const { JobStatus } = require("@textile/grpc-powergate-client/dist/ffs/rpc/rpc_pb");
const { createPow } = require("@textile/powergate-client");
// const { clonedeep } = require('lodash.clonedeep')
const fs = require('fs');
const { exec } = require("child_process");
const path = require('path');

const maxpx = 166666666666667; // max price per Gb in attoFIL
const maxfiles = 50; // max number of files to try in one go
const maxdisk = 500; // max diskspace in Gb to consume for ipfs storage
const readFrom = "/mediadrive/azure";
// const readFrom = "/lotusnode/azure";

const host = "http://0.0.0.0:6002"; // or whatever powergate instance you want
//const host = "http://172.21.0.6:6002" // or whatever powergate instance you want

function watchJob(pow, jobId, cid) {
    return new Promise(function(resolve, reject) {

        // watch all FFS events for a cid
        const logsCancel = pow.ffs.watchLogs((logEvent) => {
            console.log(`received event for cid ${logEvent.cid}:`, logEvent);
        }, cid)

        // watch the FFS job status to see the storage process progressing
        const jobsCancel = pow.ffs.watchJobs((job) => {
            if (job.status === JobStatus.JOB_STATUS_CANCELED) {
                console.log("Upload job canceled")
                reject("Upload job canceled");
            } else if (job.status === JobStatus.JOB_STATUS_FAILED) {
                console.log("Upload job failed")
                reject("Upload job failed");
            } else if (job.status === JobStatus.JOB_STATUS_SUCCESS) {
                console.log("Upload job success!");
                resolve("Upload job success.");
            }
        }, jobId)
    });
  }

function stage_ipfs(fdir, fname) {
  return new Promise(function(resolve, reject) {
    console.log("File: ", fname);
    var cmd = "docker exec mainnet_ipfs_1 ipfs add -Q /staging/" + fname
    exec(cmd, (error, stdout, stderr) => {
      if (error) {
        reject(error);
      } /*
      if (stderr) {
        reject(stderr); // Ignore as stderr streams %completion!
      } */
      console.log("Stdout: ", stdout);
      var cid = stdout.substr(0,46);
      console.log("Cid: ", cid);
      resolve(cid);
    });
  });
}

function storetoFFS(index, fname, pow, defStgCfg, fsize) {
    return new Promise(function(resolve, reject) {
        console.log("Staging fname: ", index, fname);
        stage_ipfs("", fname).then((result) => {
           var cid = result;
           console.log("Checking ...: ", index, cid);
           pow.ffs.show(cid).then((cidInfo) => {
               console.log("Current status: ", cidInfo.cidInfo.cold.filecoin);
               if(cidInfo.cidInfo.cold.filecoin.dataCid == "CID_UNDEF") {
                 console.log("Sending ...: ", index, cid, fsize);
                 // console.log("Def Cfg (pre-): ", defStgCfg.defaultStorageConfig);
                 defStgCfg.defaultStorageConfig.cold.filecoin.maxPrice = maxpx*fsize;
                 // console.log("Def Cfg (post-): ", defStgCfg.defaultStorageConfig.cold);
                 // console.log("Filecoin: ", defStgCfg.defaultStorageConfig.cold.filecoin);
                 pow.ffs.pushStorageConfig(cid, {override: true}, {storageConfig: defStgCfg.defaultStorageConfig}).then((job) => {
                    console.log("Sent for storage: ", index, cid, job); 
                    resolve({idx: index, name: fname, cid: cid, jid: job.jobId, filesize: fsize, status: "SentForStorage",  watcher: watchJob(pow, job.jobId, cid)});
                 }, (err3) => {
                   reject({error: err3, idx: index, name: fname, filesize: fsize});
                 })
               } else {
                  console.log("Already stored: ", index, cid); 
                  resolve({idx: index, name: fname, cid: cid, jid: 0, filesize: fsize, status: "AlreadyStored"});
               }
          }, (err2) => {
             if (err2.error =  "Error: stored item not found") {
               // Assume not sent yet
                 console.log("Sending(2) ...: ", index, cid, fsize);
                 // console.log("Def Cfg (pre-): ", defStgCfg.defaultStorageConfig);
                 defStgCfg.defaultStorageConfig.cold.filecoin.maxPrice = maxpx*fsize;
                 // console.log("Def Cfg (post-): ", defStgCfg.defaultStorageConfig.cold);
                 // console.log("Filecoin: ", defStgCfg.defaultStorageConfig.cold.filecoin);
                 pow.ffs.pushStorageConfig(cid, {override: true}, {storageConfig: defStgCfg.defaultStorageConfig}).then((job) => {
                    console.log("Sent for storage(2): ", index, cid, job); 
                    resolve({idx: index, name: fname, cid: cid, jid: job.jobId, filesize: fsize, status: "SentForStorage",  watcher: watchJob(pow, job.jobId, cid)});
                 }, (err3) => {
                   reject({error: err3, idx: index, name: fname, filesize: fsize});
                 })
             } else {
                console.log("Failed to get status for: ",index, fname, fsize, cid);
                reject({idx: index, name: fname, cid: cid, jid: 0, filesize: fsize, status: "StatusFetchFailed", error: err2});
             }
          })

        }, (err) => {
           console.log("Failed to stage: ", err);
           reject({error: err, idx: index, name: fname, fileize: fsize});
        })
    });
}


function fileScan(fpath, pow, defStorageConfig) {
    return new Promise(function(resolve, reject) {

      var result = true;
      var index = 1;
      var diskspace = 0;
      var promises = [];
      var watchers = [];

      console.log("Reading files from:", fpath);

      var files = fs.readdirSync(fpath);

      for (const file of files) {
          var fullname = path.join(fpath, file);
          //console.log("File: ", file);
          var fname = file;
          var stats = fs.statSync(fullname)
          var fsize = stats.size/1000000000;
          console.log("Storing ... ", index, fullname, fsize, maxpx*fsize);
          var storeFFS = storetoFFS(index, fname, pow,  defStorageConfig, fsize);
          promises.push(storeFFS);
          storeFFS.then((FFSresult) => {
            FFSresult.watcher.then((Watchresult) => {
              console.log("Watch result: ", Watchresult);
            }).catch((Watcherror) => {
                console.log("Watch error: ", Watcherror);
            });
          }).catch((FFSerror) => {
             console.log("Store FFS Error: ", FFSerror);
          });
          index += 1;
          diskspace += fsize;
          if (index > maxfiles) { break };
          if (diskspace >= maxdisk) { break };
      }

      console.log("Space required (Gbs): ", diskspace);

      Promise.all(promises).then((results) => {
          console.log("All filescan requests completed: ");
          console.log(results);
	  for (const element of results) {
		if (element.status == "SentForStorage") {
                  console.log("Watching: ", element.idx, element.cid);
 		  watchers.push(element.watcher);
                }
          };
          Promise.all(watchers).then((watchresults) => {
               resolve(true);
          }, (watcherror) => {
            console.log("Event watch error: ", watcherror);
            reject(false);
          });
        }, (error) => {
          console.log("Filescan error: ", error);
          reject(false);
      });
     })
}


async function init() {

    const pow = createPow({ host });
    //console.log("pow: ", pow);
    //console.log("pow.ffs: ", pow.ffs);

    const hlth =  await pow.health.check();
    console.log("Health: ",  hlth);

    const nodeAddr =  await pow.net.listenAddr();
    console.log("Node Address: ",  nodeAddr);

    const peers =  await pow.net.peers();
    //console.log("Peers: ",  peers);
    console.log("Peers: ",  peers.peersList.length);
    const token = "48efbd03-0b7d-40c5-a57d-4f4859673198";
    // const { token } = await pow.ffs.create();
    console.log("Token: ", token);

    const authToken = token;

    pow.setToken(authToken);

    // const { addrsList } = await pow.ffs.addrs();
    // console.log(`Address list: `, addrsList);
    // const acctInfo = await pow.ffs.info();
    // console.log(`Account Info: `, acctInfo);

    return pow

}


async function main() {

    console.log("Starting.");

    const powg = await init();

    // Get default storage config and set cold enabled to true
    var defStorageConfig = await powg.ffs.defaultStorageConfig();
    //defStorageConfig.defaultStorageConfig.cold.enabled = true;
    //defStorageConfig.defaultStorageConfig.cold.filecoin.repFactor = 1;
    //defStorageConfig.defaultStorageConfig.cold.filecoin.dealMinDuration = 60*60*24*5 //5 days;
    //defStorageConfig.defaultStorageConfig.hot.enabled = false;
    defStorageConfig.defaultStorageConfig.cold.filecoin.addr = "t3vjpcnwisfy3bxidtt77ccq45dgj6675zfifyokarrrsf36rhk66exjptljazleazuinniyftkasllkp4ppjq";
    //console.log("Def Storage Config: ", defStorageConfig.defaultStorageConfig.cold.filecoin);
    // await powg.ffs.setDefaultStorageConfig(defStorageConfig);

    // var { cidInfo } = await powg.ffs.show("QmSfGEwWWFMTkxHf82vVmctk5GWWSV5fCq2mZz42pwHPxA");
    // console.log("Current status 1: ", cidInfo.cold.filecoin);
    // var { cidInfo } = await powg.ffs.show("QmWKSx9qC4rJNTmNDHzghT7KZfLupjqvDaTHi9cxNNNQJL");
    // console.log("Current status 2: ", cidInfo.cold.filecoin);


    var result = await fileScan(readFrom, powg, defStorageConfig);
    console.log("Result:", result);


    // store the data in FFS using the default storage configuration
    // const { jobId } = await pow.ffs.pushStorageConfig(cid);

    // await watchJob(pow, jobId, cid);

    // get the current desired storage configuration for a cid (this configuration may not be realized yet)
    // const { config } = await pow.ffs.getStorageConfig(cid);

    // get the current actual storage configuration for a cid
    // const { cidInfo } = await pow.ffs.show(cid);

    // retrieve data from FFS by cid
    // const bytes = await pow.ffs.get(cid);
    // console.log("Retrieved bytes: ", bytes);
    console.log("Finished.");
}

main().catch(console.error).finally(() => process.exit());

/*

curl -X POST http://localhost:6002/health.rpc.RPCService/Check -H "content-type: application/grpc-web+proto" -H "x-grpc-web: 1"

*/
