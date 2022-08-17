// Imports
import 'dotenv/config';
import cds from '@sap/cds'
// Project imports
import { onLoad } from './app/index.js';

cds.on('loaded', () => {
    onLoad()
})
