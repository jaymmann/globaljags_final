const {Storage} = require('@google-cloud/storage');
const {Firestore} = require('@google-cloud/firestore');
const getExif = require('exif-async');
const parseDMS = require('parse-dms');
const path = require('path');
const fs = require('fs-extra');
const os = require('os');
const sharp = require('sharp');

exports.processImageAndDataV3 = async (file, context) => {
  // Explicitly setting the projectId
  const firestore = new Firestore({
    projectId: "sp24-41200-jaymmann-globaljags"
  });
  
  const storage = new Storage();
  const sourceBucket = storage.bucket(file.bucket);
  const thumbnailsBucket = storage.bucket('sp24-41200-jaymmann-gj-thumbnails');
  const finalBucket = storage.bucket('sp24-41200-jaymmann-gj-finals');
  const tempFilePath = path.join(os.tmpdir(), file.name);

  // Ensure the working directory exists
  const workingDir = path.join(os.tmpdir(), 'thumbs');
  await fs.ensureDir(workingDir);

  // Download the file to the temp path
  await sourceBucket.file(file.name).download({destination: tempFilePath});

  // Generate a thumbnail
  const thumbFilePath = path.join(workingDir, `thumb_${file.name}`);
  await sharp(tempFilePath).resize(64, 64).toFile(thumbFilePath);

  // Upload the thumbnail and the original image to their respective buckets
  await thumbnailsBucket.upload(thumbFilePath);
  await finalBucket.upload(tempFilePath);

  // Try to extract EXIF data
  let gpsData;
  try {
    const exifData = await getExif(tempFilePath);
    if (exifData && exifData.gps) {
      gpsData = getGPSCoordinates(exifData.gps);
      console.log(`Extracted GPS Data: ${JSON.stringify(gpsData)}`);
      
      // Firestore Data Writing
      const photoMetaData = {
        thumbnailUrl: `https://storage.googleapis.com/${thumbnailsBucket.name}/thumb_${file.name}`,
        finalImageUrl: `https://storage.googleapis.com/${finalBucket.name}/${file.name}`,
        latitude: gpsData.lat,
        longitude: gpsData.lon,
      };

      await firestore.collection('photos').add(photoMetaData);
      console.log('Metadata written to Firestore.');
    }
  } catch (error) {
    console.error('Error reading EXIF data or writing to Firestore:', error);
  }

  // Clean up
  await fs.remove(workingDir);
};

function getGPSCoordinates(g) {
  const latString = `${g.GPSLatitude[0]}:${g.GPSLatitude[1]}:${g.GPSLatitude[2]}${g.GPSLatitudeRef}`;
  const longString = `${g.GPSLongitude[0]}:${g.GPSLongitude[1]}:${g.GPSLongitude[2]}${g.GPSLongitudeRef}`;
  return parseDMS(`${latString} ${longString}`);
}
