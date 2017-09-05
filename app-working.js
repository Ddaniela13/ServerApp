/**
 * 
 */
var amqp = require('amqplib');
var express = require('express')();
var app= require('http').Server(express);
//var app = require('http').createServer(handler);
var io = require('socket.io')(app);
var fs = require('fs');
var _ = require('underscore');
var wpimg = require('wikipedia-image');
var Pusher = require('pusher-client');  // incoming data from Panoptes
let pusherSocket = '79e8e05ea522377ba6db';
let socket = new Pusher(pusherSocket, {
    encrypted: true
  });
let socketP = new Pusher(pusherSocket, {
    encrypted: true
  });

var amqpSocket = require('socket.io-client')('http://localhost:9002');

// var config = require('./config');

// app.timeout = 0;// trying to fix a stupid error
app.listen(9001);

var startup_date = new Date();

var active_users = 0;
var temp_user_cnt = 0;

// var keyword_filter = "";
// var language_filter = "";

var processed_msg_cnt = 0;

// don't need the global filters_made
// var filters_made = [];

// DATA STRUCTURE ds[room]= {}
// vizs=[]
// filter={}
// filter.keyword
// filter.language
// filters_made=[]

var ds = [];
var rooms = [];

function showErr(e) {
	console.error(e, e.stack);
}

function handler(req, res) {
	res.writeHead(200);
	res.end("");
}

io.sockets.setMaxListeners(0);

// SOCKET DETAILS
// Current socketnames in use:
// user_heartbeat
// active_user
// filter
// filter_keyword
// set_filter_keyword
// filter_lang
// set_filter_lang
// processed_messages;

// Hoses (Mappings from RabbitMQ)
// trends
// news
// twitter
// spinn3r
// wikipedia_revisions
// wikipedia_images
// twitter_delete
// twitter_delete_pulse

var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/roomDB');
mongoose.Promise = global.Promise;
// notification if we connect successfully to the db or if a connection error
// occurs
var db = mongoose.connection;

//img paths
var imgPath0 = '\ms.png';
var imgPath1 = '\wikipediamap(resized).png';
var imgPath2 = '\cascades.png';
var imgPath3 = '\deletedtweets(resized).png';

db.on('error', console.error.bind(console, 'connection error:'));


// reference to the schema
var roomsSchema = mongoose.Schema({
	roomID : Number,
	roomName: String,
	timeCreated : Date,
	owner : String,
	attached_vizs : Array,
	// state of the room, if it's still visible
	active : Boolean,
	description : String,
	stats : {
		view_count : Number,
		unique_participants : Array
	}
// when the room becomes inactive
// time_archived: Date
}, {
	// _id: false,
	versionKey : false
});

// compiling our schema into a Model
var Room = mongoose.model('Room', roomsSchema);

//creating a schema for the visualizations
var vizSchema= mongoose.Schema({
	vizName: String,
	picture: { data: Buffer, contentType: String },
	URL: String
	
},{
	//_id: false,
	versionKey : false
	
});

var VizModel = mongoose.model('VizModel', vizSchema); 

db.once('open', function() {
	console.log("connected to database");
	
	// empty the collection
	  VizModel.remove(function (err) {
	    if (err) throw err;

	    console.error('removed old docs from VizModel');

	    // store an img in binary in mongo
	    var a = new VizModel;
	    a.picture.data = fs.readFileSync(imgPath1);
	    a.picture.contentType = 'image/png';
	    a.vizName="Wikipedia_Map";
	    a.URL="file:///C:/Users/Dani/Documents/GitHub/wikipediaMap/index.html";
	    a.save(function (err, a) {
	      if (err) throw err;

	      console.error('saved img to mongo');

	      express.get('/Wikipedia_Map', function (req, res, next) {
	    	 VizModel.findOne({vizName: "Wikipedia_Map"}, function (err, doc) {
	          if (err) return next(err);
	          res.contentType(doc.picture.contentType);
	          res.send(doc.picture.data);
	        });
	      });

	    });
	    
	    var b = new VizModel;
	    b.picture.data = fs.readFileSync(imgPath2);
	    b.picture.contentType = 'image/png';
	    b.vizName="Twitter_Cascades";
	    b.URL="file:///C:/Users/Dani/Documents/GitHub/twitterCascades/index.html";
	    b.save(function (err, b) {
	      if (err) throw err;

	      console.error('saved img to mongo');

	      express.get('/Twitter_Cascades', function (req, res, next) {
	    	 VizModel.findOne({vizName: "Twitter_Cascades"}, function (err, doc) {
	          if (err) return next(err);
	          res.contentType(doc.picture.contentType);
	          res.send(doc.picture.data);
	        });
	      });

	    });
	    
	    var c = new VizModel;
	    c.picture.data = fs.readFileSync(imgPath3);
	    c.picture.contentType = 'image/png';
	    c.vizName="Deleted_Tweets";
	    c.URL="file:///C:/Users/Dani/Documents/GitHub/deletedTweets/index.html";
	    c.save(function (err, c) {
	      if (err) throw err;

	      console.error('saved img to mongo');

	      express.get('/Deleted_Tweets', function (req, res, next) {
	    	 VizModel.findOne({vizName: "Deleted_Tweets"}, function (err, doc) {
	          if (err) return next(err);
	          res.contentType(doc.picture.contentType);
	          res.send(doc.picture.data);
	        });
	      });

	    });
	    
	    var d = new VizModel;
	    d.picture.data = fs.readFileSync(imgPath0);
	    d.picture.contentType = 'image/png';
	    d.vizName="Filter";
	    d.URL="file:///C:/Users/Dani/Documents/GitHub/ms-filter/index.html";
	    d.save(function (err, d) {
	      if (err) throw err;

	      console.error('saved img to mongo');

	      express.get('/Filter', function (req, res, next) {
	    	 VizModel.findOne({vizName: "Filter"}, function (err, doc) {
	          if (err) return next(err);
	          res.contentType(doc.picture.contentType);
	          res.send(doc.picture.data);
	        });
	      });

	    });
	    
	    
	  });

});

function updateRoomID(socket) {
	Room.findOne().sort({
		$natural : -1
	}).exec(function(err, result) {
		if (err)
			return console.error(err);
		if (result == null) {
			var id = 1;
			try {
				socket.emit("returnedRoomID", id);
			} catch (e) {
				console.log("not emitting -returnedRoomID- in if");
			}
		} else {
			try {
				var newid = result.roomID;
				newid++;
				socket.emit("returnedRoomID", newid);
			} catch (e) {
				console.log("not emitting -returnedRoomID-");
			}
		}

	});
}

function getActiveRooms(socket){
	Room.find().sort({$natural: -1}).exec(function(err,response){
		if(err) return console.error(err);
		try{
			socket.emit("activeRooms", response);
		}catch(e){
			console.log("not emitting activeRooms");
		}
	});
}

function getRoomDetails(data){
	Room.findOne({ roomID: data.id }, function(err, result){
		if(err) return console.error(err);
		try{
			data.socket.emit("roomDetails", result);
		}catch(e){
			console.log("not emitting roomDetails event");
		}
	});
}

function getAttachedVizs(data){
	Room.findOne({roomID:data.roomID}, function(err,docs){
	
		var vizs=docs.attached_vizs;
	
	
	
	VizModel.find({
	    'vizName': { $in: vizs}
	}, function(err, docs){
		if(err) console.error(err);
		data.socket.emit('vizsURLS', {"vizDocs": docs, "roomID":data.roomID});
	    console.log(docs);
	});
	
	});
	
//	for(int i=0; i<data.attached_vizs.length; i++){
//		VizModel.findOne({vizName : data.attached_vizs[i]}, "URL", "vizName", function(err, response){
//			if(err) return console.error(err);
//			response.URL= response.URL+ "?room=" + data.roomID + "&vizName=" + response.vizName;
//			vizs[i]=response;
//			console.log(i+ " " +vizs[i]);
//		});
//		
//		
//	}
//	
//	if(vizs.length==data.attached_vizs.length){
//		console.log("Array of vizs: ");
//		console.log(vizs.length);
//	}else{
//		console.log("stupid thing");
//	}
}

io.on('connection', function(socket) {
	
	socket.on("getRoomDetails", function(data){
		console.log("getRoomDetails event triggered");
		getRoomDetails({"socket":socket, "id": data});
	})

	socket.on("updateRoom", function(data) {
		console.log("updateRoom event triggered");
		updateRoomID(socket);
	});

	socket.on("newRoom", function(data) {
		console.log("newRoom event triggered");

		var doc = new Room({
			roomID : data.roomID,
			roomName: data.roomName,
			timeCreated : data.currentDate,
			owner : "",
			attached_vizs : data.attached_vizs,
			active : data.active,
			description : data.roomDesc,
			stats : {}
		});

		doc.save(function(err, doc) {
			if (err)
				return console.error(err);
		});
		
		//getAttachedVizs(data);

		console.log("NewRoom added to db");
		socket.emit("roomCreated", "");
		console.log(rooms);

	});
	
	socket.on('getActiveRooms', function(data){
		console.log("getActiveRooms triggered");
		getActiveRooms(socket);
	});
	
	socket.on('getURLS', function(data){
		getAttachedVizs({"socket": socket, "roomID": data});
	});

	socket.on('newViz', function(data) {
		var newViz = {};
		newViz.vizName = data.vizName;
		newViz.currentDate = data.currentDate;

		console.log("");
		console.log("NewViz created");

		// if room exists
		if (ds[data.room] != undefined) {

			// push newViz in there
			ds[data.room].vizs.push(newViz);
			console.log(data.room + " already exists and " + newViz.vizName
					+ " is in here");
			// if room doesn't exist
		} else {
			ds[data.room] = {};
			// add this new room the array of rooms
			rooms.push(data.room);
			ds[data.room].vizs = [];
			ds[data.room].vizs.push(newViz);
			// add an empty filter to the newly created room
			ds[data.room].filter = {
				// set the filter to this new room to false
				"filter" : false,
				"keyword_filter" : "",
				"language_filter" : ""
			};
			// array for filters applied to this room
			ds[data.room].filters_made = [];
			console.log(data.room + " was just created and " + newViz.vizName
					+ " was added to this room now");
			// socket.emit("addNewRoom", data.room);

			// socket.emit("filter", filter); // emit the current state to
			// this client

		}

		// finally send newViz to the specified room
		socket.join(data.room);
		// socket.broadcast.to(data.room).emit('newVizJoined',
		// newViz.vizName);
		// show the filters in that room
		socket.emit('existing_filters', ds[data.room].filters_made);
		io.sockets.to(data.room).emit("set_filter_keyword",
				ds[data.room].filter.keyword_filter);
		console.log("set_filter keyword emitted");
		io.sockets.to(data.room).emit("set_filter_lang",
				ds[data.room].filter.language_filter);
		console.log("set_filter lang emitted");
		// emit this to the filter so that the filter checks the room and
		// emits the right filter to the viz
		// io.sockets.to(data.room).emit('checkRoom', {"vizName":
		// newViz.vizName, "room": data.room});
		console.log("The vizs in this room are: " + "\n" + ds[data.room].vizs);
		console.log("Current Date HEREEEE:" + newViz.currentDate);
	});

	// my version of 'filter'
	// should receive the filter and also the room that the filter applies
	// to
	socket.on("filter", function(data) {
		console.log("Filter updated:", data.newFilter);
		// if room exists update filter, combine it and send to clients in
		// the
		// specified room
		if (ds[data.room] != undefined) {
			_.extend(ds[data.room].filter, data.newFilter);
			var tempFilter = ds[data.room].filter;
			io.sockets.to(data.room).emit("filter", tempFilter.filter);
			io.sockets.to(data.room).emit("set_filter_keyword",
					tempFilter.keyword_filter);
			io.sockets.to(data.room).emit("set_filter_lang",
					tempFilter.language_filter);
		} else {
			console.log("Room doesn't exist");
		}
	});

	socket.on('get_filter_list', function(room) {
		// update with the last few items of the filter_list in that room
		try {
			var last_filters = ds[room].filters_made;
			sendLastFilterItems({
				"room" : room,
				"last_filters" : last_filters.slice((last_filters.length - 3),
						(last_filters.length - 1))
			});
			console.log("Filter list: " + "\n" + ds[room].filters_made);
		} catch (e) {
			// Might be an empty list...
		}
	});

	// should also receive the room along with the newFilter
	socket.on('filter_keyword', function(data) {
		console.log("Filter_keyword updated:", data.newFilter);
		if (ds[data.room] != undefined) {
			var tempFilter = ds[data.room].filter;
			tempFilter.filter = true;
			tempFilter.keyword_filter = data.newFilter;
			// emit to the sockets in the room
			io.sockets.to(data.room).emit("filter", tempFilter.filter);
			io.sockets.to(data.room).emit("set_filter_keyword",
					tempFilter.keyword_filter);

			// add the new filter to the filters_made
			addToFilterList(data.room, "keyword", tempFilter.keyword_filter);

		} else {
			console.log("Room doesn't exist");
		}
	});

	// receive a filter update, combine it and sent it to the clients in the
	// specified ROOM!!
	socket.on('filter_lang', function(data) {
		if (ds[data.room] != undefined) {
			var tempFilter = ds[data.room].filter;
			tempFilter.filter = true;
			tempFilter.language_filter = data.newFilter;
			io.sockets.to(data.room).emit("filter", tempFilter.filter);
			io.sockets.to(data.room).emit("set_filter_lang",
					tempFilter.language_filter);

			addToFilterList(data.room, "language", tempFilter.language_filter);
			console.log("Filter_language updated:", data.newFilter);

		} else {
			console.log("Room doesn't exist");
		}
	});

	// new ms user...
	// receive a filter update, combine it and send to ALL clients
	socket.on('active_user', function(newFilter) {
		// console.log("filter updated:", newFilter);
		// console.log("emitting filter:", filter);
		++temp_user_cnt;
	});
	
//	socket.on('classification', function(data){
//		console.log("classification event received");
//	});

});

amqpSocket.on('spinn3r', function(data){
	emitMsg('spinn3r', data);
	console.log('on spinn3r');
	//console.log(data);
});

amqpSocket.on('wikipedia_revisions', function(data){
	emitMsg('wikipedia_revisions', data);
});

amqpSocket.on('twitter_delete', function(data){
	emitMsg('twitter_delete', data);
});

amqpSocket.on('twitter_delete_pulse', function(data){
	emitMsg('twitter_delete_pulse', data);
});

amqpSocket.on('twitter', function(data){
	emitMsg('twitter', data);
});

amqpSocket.on('zooniverse_classifications', function(data){
	emitMsg('zooniverse_classifications', data);
});

amqpSocket.on('zooniverse_talk', function(data){
	emitMsg('zooniverse_talk', data);
});

amqpSocket.on('twitter_moocs', function(data){
	emitMsg('twitter_moocs', data);
});

amqpSocket.on('twitter_uk_southampton', function(data){
	emitMsg('twitter_uk_southampton', data);
});

function emit_processed_message_count() {
	if (processed_msg_cnt > 3000000) {
		processed_msg_cnt = 0;
	}
	try {
		io.emit("processed_msg_cnt", processed_msg_cnt);
	} catch (e) {
		console.log("failing here" + e)
	}
}
// reset filters every 60 seconds - just for sanity...
// var processed_msgcnt_interval =
// setInterval(function(){emit_processed_message_count()}, 2000);

// send a list of initial items
function sendLastFilterItems(data) {
	io.sockets.to(data.room).emit("existing_filters", data.last_filters);
}

// add to the current list of filters....IN THAT ROOM!!
// modified this function to also send the room as one of the parameters
function addToFilterList(room, type, filter_string) {
	var date = new Date();
	var data = {
		"type" : type,
		"filter" : filter_string,
		"timestamp" : date
	}
	try {
		ds[room].filters_made.push(data);
	} catch (e) {
		console.log("Failing here, room might not exist");
	}
	// change this emit to know what room we're talking about
	io.sockets.to(room).emit("new_filter_item", data);
}

// reset all filters
function resetKeywords(room) {

	keyword_filter = "";
	language_filter = "";

	// let the clients in that room know
	io.sockets.to(room).emit("set_filter_keyword", keyword_filter);
	io.sockets.to(room).emit("set_filter_lang", language_filter);
}

// reset filters every 60 seconds - just for sanity...
var resetKeywords_interval = setInterval(function(room) {
	resetKeywords(room)
}, 1200000);

// Update the master count with the temp count
function updateUserCount() {
	active_users = temp_user_cnt;
}

// want to send a heartbeat to all users to see how many are still connected!
function checkForUsers() {
	// send a heartbeat
	temp_user_cnt = 0;
	io.emit("user_heartbeat", "blabla");
}

// want to send a heartbeat to all users to see how many are still connected!
function emitUserCount() {
	// send a heartbeat
	io.emit("active_user_count", active_users);
}

// These Control the User Count Details
var checkForUsers_interval = setInterval(function() {
	checkForUsers()
}, 1000);
var updateUserCount_interval = setInterval(function() {
	updateUserCount()
}, 2000);
var emit_userCount = setInterval(function() {
	emitUserCount()
}, 1000);

// check filters for a particular room
function checkFilters(msg, room) {

	var match_keyword = true;
	var match_lang = true;

	var tempFilter = ds[room].filter;
	// is the filter enabled?
	if (tempFilter.filter) {

		// check for keyword filter
		if (msg.content.toString().indexOf(tempFilter.keyword_filter) > -1) {
			match_keyword = true;
		} else {
			match_keyword = false;
		}

		if (tempFilter.language_filter.length > 0) {

			// var data = JSON.parse(msg.content.toString());
			try {
				if (msg.content.toString().indexOf(
						'"' + tempFilter.language_filter + '"') > -1) {
					match_lang = true;
				} else {
					match_lang = false;
				}
			} catch (e) {
				console.log(e)
				match_lang = false;
			}
		}

		if (match_lang && match_keyword) {
			return true;
		} else {
			return false;
		}

	} else {

		return true;
	}

}

// here we worry about the message sending
// We perform raw filtering here!
var emitMsg = function(outName, msg) {
	try {
		// ++processed_msg_cnt;

		// loop through all the rooms in the ds and apply the right filters to
		// each room
		for (var room = 0; room < rooms.length; room++) {
			if (checkFilters(msg, rooms[room])) {

				var data = JSON.parse(msg.content.toString());
				// emit data to that room?
				io.to(rooms[room]).emit(outName, data);

				// console.log(outName);
				// make the revisions images feed
				if (outName == "wikipedia_revisions") {
					// console.log(ring());
					var page_url = data.wikipedia_page_url;
					if (page_url) {
						wpimg(page_url).then(function(image) {
							if (image && image != "") {
								// emit to the entire room?
								io.emit('wikipedia_images', {
									"image_url" : image,
									"data" : data
								});
							}

						}, function(e) {
							// error querying etc
						});
					}
				}

				// end of filter
			}
		}
		// do a raw match on the message

	} catch (e) {
		//
	}
}

//Panoptes stuff
let classificationEvents = socketP.subscribe('panoptes');
classificationEvents.bind('classification',
  function(data) {
	//console.log("classification event");
	io.sockets.emit('classification', data);
  }
);

let commentEvents = socketP.subscribe('talk');
commentEvents.bind('comment',
  function (data) {
	//console.log("comment event");
    io.sockets.emit('comment', data);
  }
);

var connectQueue = function(queueName, outName) {
	return amqp.connect("amqp://admin:Sociam2015@sotonwo.cloudapp.net:5672")
			.then(
					function(conn) {

						process.once('SIGINT', function() {
							conn.close();
						});
						return conn.createChannel().then(
								function(ch) {
									var ok = ch.assertExchange(queueName,
											'fanout', {
												durable : false
											});

									ok = ok.then(function() {
										return ch.assertQueue('', {
											exclusive : true
										});
									});

									ok = ok.then(function(qok) {
										return ch.bindQueue(qok.queue,
												queueName, '').then(function() {
											return qok.queue;
										});
									});

									ok = ok.then(function(queue) {
										// function(msg,room)
										return ch.consume(queue, function(msg) {
											emitMsg(outName, msg);
										}, {
											noAck : true
										});
									});

									return ok;
								});
					});
};

//var connectQueueTwo = function(queueName, outName) {
//	return amqp.connect("amqp://wsi-h1.soton.ac.uk").then(function(conn) {
//
//		process.once('SIGINT', function() {
//			conn.close();
//		});
//		return conn.createChannel().then(function(ch) {
//			var ok = ch.assertExchange(queueName, 'fanout', {
//				durable : false
//			});
//
//			ok = ok.then(function() {
//				return ch.assertQueue('', {
//					exclusive : true
//				});
//			});
//
//			ok = ok.then(function(qok) {
//				return ch.bindQueue(qok.queue, queueName, '').then(function() {
//					return qok.queue;
//				});
//			});
//
//			ok = ok.then(function(queue) {
//				// function(msg, room)
//				return ch.consume(queue, function(msg) {
//					emitMsg(outName, msg);
//				}, {
//					noAck : true
//				});
//			});
//
//			return ok;
//		});
//	});
//};
//
//// var connect = connectQueue("wikipedia_hose", "wikipedia_revisions");
//// connect = connect.then(function() { return connectQueue("twitter_hose",
//// "tweets"); }, showErr);
//// connect = connect.then(function() { return connectQueue("trends_hose",
//// "trends"); }, showErr);
//
//// for the larger spinn3r connection
//var connect = connectQueueTwo("twitter_double", "spinn3r");
//
//// connect = connect.then(function() { return connectQueueTwo("twitter_double",
//// "spinn3r"); }, showErr);
//// Wiki on the cluster
//connect = connect.then(function() {
//	return connectQueueTwo("wikipedia_hose", "wikipedia_revisions");
//}, showErr);
//
//connect = connect.then(function() {
//	return connectQueueTwo("twitter_delete_hose", "twitter_delete");
//}, showErr);
//
//connect = connect.then(
//		function() {
//			return connectQueueTwo("twitter_delete_pulse_hose",
//					"twitter_delete_pulse");
//		}, showErr);
//
//connect = connect.then(function() {
//	return connectQueueTwo("twitter_double", "twitter");
//}, showErr);
//
//// connect = connect.then(function() { return connectQueueTwo("news_hose",
//// "news"); }, showErr);
//
//connect = connect.then(function() {
//	return connectQueueTwo("zooniverse_classifications",
//			"zooniverse_classifications");
//}, showErr);
//connect = connect.then(function() {
//	return connectQueueTwo("zooniverse_talk", "zooniverse_talk");
//}, showErr);
//
//connect = connect.then(function() {
//	return connectQueueTwo("twitter_moocs", "twitter_moocs");
//}, showErr);
//// connect = connect.then(function() { return connectQueueTwo("twitter_moocs",
//// "spinn3r"); }, showErr);
//connect = connect.then(function() {
//	return connectQueueTwo("twitter_uk_southampton", "spinn3r");
//}, showErr);
//connect = connect.then(function() {
//	return connectQueueTwo("twitter_uk_southampton", "twitter_uk_southampton");
//}, showErr);
//
//// Finally, are we ready?
//connect = connect.then(function() {
//	console.log("Ready at:" + startup_date);
//}, showErr);