'use strict'
const randomBytes = require('crypto').randomBytes
var toHex = Array(256)
for (var val = 0; val < 256; val++) 
  toHex[val] = (val + 0x100).toString(16).substr(1)
function uuid4() {
  var buf = randomBytes(16)
  buf[6] = (buf[6] & 0x0f) | 0x40
  buf[8] = (buf[8] & 0x3f) | 0x80
  var i=0
  return    toHex[buf[i++]] + toHex[buf[i++]] +
            toHex[buf[i++]] + toHex[buf[i++]] + '-' +
            toHex[buf[i++]] + toHex[buf[i++]] + '-' +
            toHex[buf[i++]] + toHex[buf[i++]] + '-' +
            toHex[buf[i++]] + toHex[buf[i++]] + '-' +
            toHex[buf[i++]] + toHex[buf[i++]] +
            toHex[buf[i++]] + toHex[buf[i++]] +
            toHex[buf[i++]] + toHex[buf[i++]]
}
console.log(uuid4())