// SMTP Balance
// This is an altered plugin of SMTP proxy.
// Plugin will oepn a connection to a list of smtp servers
// it will track the number of connections and forward to the least used host
//
// 
// 
// Original smtp_proxy notes:
// Proxy to an SMTP server
// Opens the connection to the ongoing SMTP server at MAIL FROM time
// and passes back any errors seen on the ongoing server to the
// originating server.

var smtp_client_mod = require('./smtp_client');


exports.hook_connect = function (next, connection) {
	var self = this;
	var config = this.config.get('smtp_balance.json');
	var snotes = connection.server.notes;
	if(!snotes.cluster){
		snotes.cluster = [];
		var uniq = new Date;
		for(var x in config.hosts){
			config.hosts[x].uniq_id = uniq;
			uniq++;
			config.hosts[x].active_connections = 0;
			config.hosts[x].total_connections = 0;
			snotes.cluster.push(config.hosts[x]);
		}
	}
	snotes.cluster.sort(function(a,b) {return (a.total_connections > b.total_connections) ? 1 : ((b.total_connections > a.total_connection) ? -1 : 0);});
	snotes.cluster[0].active_connections++;
	snotes.cluster[0].total_connections++;
	connection.notes.config = snotes.cluster[0];
	return next();
}

exports.hook_mail = function (next, connection, params) {
    var config = {};
	config.main = connection.notes.config;
    connection.loginfo(this, "proxying to " + config.main.host + ":" + config.main.port + " [active cons: "+ config.main.active_connections +"]");
    var self = this;
    smtp_client_mod.get_client_plugin(this, connection, config, function (err, smtp_client) {
        connection.notes.smtp_client = smtp_client;
        smtp_client.next = next;

        smtp_client.on('mail', smtp_client.call_next);
        smtp_client.on('rcpt', smtp_client.call_next);
        smtp_client.on('data', smtp_client.call_next);

        smtp_client.on('dot', function () {
            delete connection.notes.smtp_client;
        });

        smtp_client.on('error', function () {
            delete connection.notes.smtp_client;
        });

        smtp_client.on('bad_code', function (code, msg) {
            smtp_client.call_next(code.match(/^4/) ? DENYSOFT : DENY,
                smtp_client.response.slice());

            if (smtp_client.command !== 'rcpt') {
                // errors are OK for rcpt, but nothing else
                // this can also happen if the destination server
                // times out, but that is okay.
                connection.loginfo(self, "message denied, proxying failed");
                smtp_client.release();
                delete connection.notes.smtp_client;
            }
        });
    });
};

exports.hook_rcpt_ok = function (next, connection, recipient) {
    var smtp_client = connection.notes.smtp_client;
    if (!smtp_client) return next();
    smtp_client.next = next;
    smtp_client.send_command('RCPT', 'TO:' + recipient);
};

exports.hook_data = function (next, connection) {
    var smtp_client = connection.notes.smtp_client;
    if (!smtp_client) return next();
    smtp_client.next = next;
    smtp_client.send_command("DATA");
};

exports.hook_queue = function (next, connection) {
    var smtp_client = connection.notes.smtp_client;
    if (!smtp_client) return next();
    smtp_client.next = next;
    smtp_client.start_data(connection.transaction.data_lines);
};

exports.hook_rset = function (next, connection) {
    var smtp_client = connection.notes.smtp_client;
    if (!smtp_client) return next();
    smtp_client.release();
    delete connection.notes.smtp_client;
    next();
}

exports.hook_quit = exports.hook_rset;

exports.hook_disconnect = function (next, connection) {
    var smtp_client = connection.notes.smtp_client;
	var snotes = connection.server.notes;
	for(var x in snotes.cluster){
		if(snotes.cluster[x].uniq_id === connection.notes.config.uniq_id){
			snotes.cluster[x].active_connections--;
		}
	}
    if (!smtp_client) return next();
    smtp_client.release();
    delete connection.notes.smtp_client;
    smtp_client.call_next();
    next();
};
