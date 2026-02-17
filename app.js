// app.js
const express = require('express');
const {Firestore} = require('@google-cloud/firestore');
const {OAuth2Client} = require('google-auth-library');

const app = express();
app.use(express.json());

const EXPECT_AUDIENCES = (process.env.EXPECT_AUDIENCES || '').split(',').map(s => s.trim()).filter(Boolean);
const PORT = process.env.PORT || 8080;
const projectId = process.env.GCP_PROJECT || process.env.GCLOUD_PROJECT || 'closer-video-similarity';

const firestore = new Firestore({projectId});
const oauth2Client = new OAuth2Client();

async function verifyIdToken(token) {
  if (!token) throw new Error('no-token');
  for (const aud of EXPECT_AUDIENCES) {
    try {
      const ticket = await oauth2Client.verifyIdToken({idToken: token, audience: aud});
      return ticket.getPayload();
    } catch (err) {
      // try next audience
    }
  }
  throw new Error('invalid-token');
}

app.get('/health', (req, res) => res.status(200).send('ok'));

app.post('/run', async (req, res) => {
  try {
    const authHeader = req.get('Authorization') || '';
    const match = authHeader.match(/^Bearer (.+)$/);
    if (!match) return res.status(401).json({error: 'missing-token'});

    const idToken = match[1];
    await verifyIdToken(idToken);

    // Fetch brands (add paging if collection is large)
    const brandsSnap = await firestore.collection('brands').get();
    const ops = [];
    const now = new Date().toISOString();

    for (const doc of brandsSnap.docs) {
      const brand = doc.data();
      const brandId = doc.id;

      // Example transformation - customize as needed
      const adDocRef = firestore.collection('ads').doc(`ad_${brandId}`);
      const adPayload = {
        brandId,
        brandName: brand.name || null,
        fetchedAt: now,
        source: 'ads-fetcher-service'
      };

      ops.push(adDocRef.set(adPayload, {merge: true}));
    }

    await Promise.all(ops);
    return res.json({status: 'ok', processed: ops.length});
  } catch (err) {
    console.error('run error', err);
    if (err.message === 'no-token' || err.message === 'invalid-token') {
      return res.status(401).json({error: err.message});
    }
    return res.status(500).json({error: err.message});
  }
});

app.listen(PORT, () => {
  console.log(`ads-fetcher-service listening on ${PORT}`);
});
