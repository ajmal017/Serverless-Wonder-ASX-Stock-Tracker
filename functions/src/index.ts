import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin'
import PubSub from '@google-cloud/pubsub'
import axios from 'axios'

// // Start writing Firebase Functions
// // https://firebase.google.com/docs/functions/typescript

// INIT DB
export const db = admin.firestore()
db.settings({timestampsInSnapshots: true});

// CONSTS
const TICKER_WATCH_PUBSUB_TOPIC = ""

const axiosConfig = {
    headers: {'User-Agent': 'Chrome/67.0.3396.87'}
}

const getTickerDataAPIPath = ({ code="BHP.AX", interval="1m", range="7d", version="v8"}) => `https://query1.finance.yahoo.com/${version}/finance/chart/${code}?region=AU&lang=en-AU&includePrePost=true&interval=${interval}&range=${range}&corsDomain=au.finance.yahoo.com&.tsrc=finance`

export const publishToPubSubTopic = (pubSubTopic) => (message: Object, attributes={}) => {
    const pubsub = new PubSub();
    const topic = pubsub.topic(pubSubTopic);
    const publisher = topic.publisher();
    const bufferedMessage = Buffer.from(JSON.stringify(message));
    return publisher.publish(bufferedMessage, attributes)
}

export const initialiseTickerWatchers = functions.https.onRequest(async (request, response) => {
    // get ticker data / codes from DB -> only for tickers marked as isWatched
    const tickerCodeRef = db.collection("tickers_to_watch").where('isWatched',"==",true)

    // loop over organisations to prepare pubsub message contents
    const tickerCodes = await tickerCodeRef.get().then(snapshot => {
        let arr = []
        snapshot.forEach(doc => {
            arr=[...arr,{id: doc.id}]
        })
        return arr
    })

    // send pubsub messages to topic
    await Promise.all(tickerCodes.map(item=>publishToPubSubTopic(TICKER_WATCH_PUBSUB_TOPIC)({TICKER_ID: item.code})))

    response.send("Tickers monitored!");
});

exports.listenToTickerWatchPubSubTopic = functions.pubsub.topic('TICKER_WATCH_PUBSUB_TOPIC').onPublish(async (message) => {
    // extract message data
    const messageData = message.json
    const { code, calledOn } = messageData
    const tickerPath = getTickerDataAPIPath({ code })
    // init data poll
    const data = await axios.get(tickerPath, axiosConfig).then(res=>res.data)

    // save data to DB
    const dataPipe = (input) => input

    const batch = db.batch();
    const ref = db.collection('stock-data')

    const dataPiped = dataPipe(data)

    dataPiped.forEach(k=>{
        const { index, ...spread } = k
        batch.set(ref.doc(index),spread)
    })

    await batch.commit()
    // exit gracefully
    return true

  });