const { JobStatus } = require("@textile/grpc-powergate-client/dist/ffs/rpc/rpc_pb");
const { createPow } = require("@textile/powergate-client");
const fs = require('fs');
const { exec } = require("child_process");
// const { promisify } = require('util');
// const exec = promisify(require('child_process').exec);

const host = "http://0.0.0.0:6002" // or whatever powergate instance you want
//const host = "http://172.21.0.6:6002" // or whatever powergate instance you want
//const host = "http://172.21.0.3:6002" // or whatever powergate instance you want

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
                resolve();
            }
        }, jobId)
    });
  }


function stage_ipfs(fdir, fname) {
  return new Promise(function(resolve, reject) {

    var cmd = "docker exec mainnet_ipfs_1 ipfs add /staging/" + fname
    exec(cmd, (error, stdout, stderr) => {
      if (error) {
        reject(error);
      } /*
      if (stderr) {
        reject(stderr); // Ignore as stderr streams %completion!
      } */
      var cid = stdout.substr(6,46);
      console.log("Cid: ", cid);
      resolve(cid);
    });
  });
}


async function main() {

    console.log("Starting."); 

    const pow = createPow({ host });
    // console.log("pow: ", pow);
    // console.log("pow.ffs: ", pow.ffs);

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
    
        // const { peersList } = await pow.net.peers()
        // console.log(`Peer list: `, peersList);

    pow.setToken(authToken);

    const { addrsList } = await pow.ffs.addrs();
    console.log(`Address list: `, addrsList);
    const acctInfo  = await pow.ffs.info();
    console.log(`Account Info: `, acctInfo);


    // create a new address associated with your ffs instance
    // const { addr } = await pow.ffs.newAddr("my new addr");
    // const addr = "t3w3bsi65jkcse4mwchij64dumvmyracrahxts4b32jxgdkbckvntxayc36emsm4w5wpyyxpoobzmauqxylszq";
    // console.log("My addr: ", addr);


    // cache data in IPFS in preparation to store it using FFS
    // const buffer = fs.readFileSync('/export/file_example_MP4_1920_18MG.mp4'); // Need to add a volume to the powergate instance
    // const buffer = fs.readFileSync('Goggles.odt');
    // console.log("Setting buffer.");
    // const buffer = fs.readFileSync("/mediadrive/test/A_CAM0069.MXF");
    // const buffer = fs.readFileSync("/mediadrive/azure/75407b11-86e9-4e62-af7a-df5ed6882488_52.mp4");
    const fname = "b36543d7-d79d-44e9-b89b-77170efd5ce6_50.mp4";

    // console.log("Set buffer.");
    //const { cid } = await pow.ffs.stage(buffer);
    const cid = await stage_ipfs ("", fname); 

    // const cid = "QmZGXusiEBXTggWS8VgwBdMH3EKURcTHcmHr4DxCf6JYZd"; // Goggles.odt [10kb]
    // const cid = "QmQPQ6Zjrf6PRyrnaENp9x2mzZuoiBtxcgoYgMKz1VVzQE"; // openplotter [1.1Gb]
    // const cid = "QmfU55PgPMgmb3iFoWWcJbKkXHWWRYmGpbh1pEKLi6u57k"; // 2281bda4-0438-466b-a776-46769a4a0fc7_48.mp4
    // const cid = "QmXN5hQP3HkcKCLnqv8QizsEmhWYLLjBpaGzeYgV6dbudA"; // ident.mp4 [20kb]
    // const cid = "QmNh4HXE5sPA1s9pGEnAkmBG4H4bdUbBwxhStnEA2AWswG"; // file_examp ... mp4 [20Mb]
    // const cid = "QmPMk9rP2yxrBLNPZSvkZc54nHFjAN4JHRZkjhrdjmgvUK"; // app.js
    console.log("Storing cid: ", cid);

    const defConfig  = await pow.ffs.defaultStorageConfig(authToken);
    console.log("Default config: ", defConfig)

    // store the data in FFS using the default storage configuration
    const { jobId } = await pow.ffs.pushStorageConfig(cid, {override:true});
    console.log("Running job: ", jobId);

    // const jobId = "2151503a-cb4d-4bf6-b14d-5ab44fd04c12";

    await watchJob(pow, jobId, cid);

    // get the current desired storage configuration for a cid (this configuration may not be realized yet)
   //  const { config } = await pow.ffs.getStorageConfig(cid);

    // get the current actual storage configuration for a cid
    // const { cidInfo } = await pow.ffs.show(cid);

    // retrieve data from FFS by cid
    // const bytes = await pow.ffs.get(cid);
    // console.log("Retrieved bytes: ", bytes);
}

main().catch(console.error).finally(() => process.exit());

/*

curl -X POST http://localhost:6002/health.rpc.RPCService/Check -H "content-type: application/grpc-web+proto" -H "x-grpc-web: 1"

*/
