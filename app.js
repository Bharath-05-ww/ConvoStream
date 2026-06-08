const { Server } = require("socket.io");
const express = require('express')
const { createServer } = require('node:http')
const { join } = require('node:path')
const sqlite3  = require('sqlite3').verbose();
const { open } = require('sqlite');
const { availableParallelism } = require('node:os');
const cluster = require('node:cluster')
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter');
const { v4: uuidv4 } = require('uuid');

if(cluster.isPrimary){
    const numCPU = availableParallelism(); 
    for(let i=0;i<numCPU;i++){
        cluster.fork({
            PORT:3000+i
        });
    }
    return setupPrimary();
}

async function main(){
    const app = express()
    const server = createServer(app);
    const io = new Server(server,{
        connectionStateRecovery:{},
        adapter:createAdapter()
    });

    const port = process.env.PORT;
    server.listen(port,() =>{
        console.log(`Server running at http://localhost:${port}`);
    })

    const db = await open({
        filename:'chat.db',
        driver:sqlite3.Database
    });

    await db.exec(`
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_offset TEXT UNIQUE,
            username TEXT,
            content TEXT,
            timestamp TEXT
        );
  `);
  await db.exec(`
        CREATE TABLE IF NOT EXISTS private_messages(
            messageId TEXT,
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            senderId TEXT,
            receiver TEXT,
            receiverId TEXT,
            content TEXT,
            timestamp TEXT,
            status TEXT DEFAULT 'sent'
        )
`);
await db.exec(`
CREATE TABLE IF NOT EXISTS users(
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE
)
`);
    
app.use(express.static(__dirname));
app.get('/',(req,res)=>{
    res.sendFile(join(__dirname,'index.html'));
});


const onlineUsers = new Map();
io.on('connection',async (socket)=>{
    
    const username =socket.handshake.auth.username || "Anonymous";

    let user = await db.get(
        `SELECT * FROM users WHERE username=?`,
        username
    );

    if(!user){

        const newId = uuidv4();

        await db.run(
            `INSERT INTO users(id,username)
            VALUES(?,?)`,
            newId,
            username
        );

        user = {
            id:newId,
            username
        };
    }

    socket.userId = user.id;
    socket.emit(
        'user identity',
        {
            userId:socket.userId
        }
    );
    socket.username = user.username;

    socket.broadcast.emit('system message',`${socket.username} joined the chat`);
    onlineUsers.set(socket.userId,{
        id:socket.userId,
        socketId:socket.id,
        username:socket.username
    });
   
    io.emit('online users',[...onlineUsers.values()]);
    socket.on('chat message',async (msg,clientOffset,callback) =>{
        console.log("Server Got:",msg);
        console.log("Message Received",Date.now());

        let result;
        const msgtime = new Date().toLocaleTimeString([],{
                hour: '2-digit',
                minute: '2-digit'
            });
        try{
            result = await db.run('INSERT INTO messages (username,content,client_offset,timestamp) VALUES (?,?,?,?)',socket.username,msg,clientOffset,msgtime);
          
        }
        catch(e){
            if(e.errno === 19){ 

                callback();
            }
            else{
                console.error(e);
            }
            return;
        }
        const data = {
            username:socket.username,
            message:msg,
            id:result.lastID,
            timestamp:msgtime
        }
        
        socket.broadcast.emit('chat message',data);
        callback();
    });
    socket.on('load previews', async (callback)=>{

        const rows = await db.all(`
            SELECT *
            FROM private_messages
            WHERE senderId=? OR receiverId=?
            ORDER BY id DESC
        `,
        socket.userId,
        socket.userId
        );

        callback(rows);

   });

    socket.on('typing',()=>{
        console.log(socket.username,'typing');
        socket.broadcast.emit('typing',socket.username);
    });
    socket.on('stop typing',()=>{
        console.log(socket.username,'stop typing');
        socket.broadcast.emit('stop typing',socket.username);
    });
    socket.on('private message',async (data)=>{
        console.log("Server received:", data);
        const timestamp = new Date().toLocaleTimeString([],{
            hour:'2-digit',
            minute:'2-digit'
        });
        const receiverUser = onlineUsers.get(data.to);
        await db.run(
        `
            INSERT INTO private_messages
            (messageId,sender,senderId,receiver,receiverId,content,timestamp,status)
            VALUES(?,?,?,?,?,?,?,?)
            `,
            data.messageId,
            socket.username,
            socket.userId,
            receiverUser?.username || "Unknown",
            data.to,
            data.message,
            timestamp,
            'sent'
        );
        // const allRows = await db.all(`
        //     SELECT * FROM private_messages
        //     `);

        
        if(receiverUser){
            io.to(receiverUser.socketId).emit('private message',{
                // id: data.id,
                from: socket.username,
                message: data.message,
                fromId: socket.userId,
                messageId:data.messageId
            });
}
        socket.emit('private message',{
            from: socket.username,
            message: data.message,
            fromId: socket.userId
        })
    })

    socket.on('load private chat', async (data, callback) => {
       
        const rows = await db.all(
        `
        SELECT *
        FROM private_messages
        WHERE
        (senderId=? AND receiverId=?)
        OR
        (senderId=? AND receiverId=?)
        ORDER BY id
        `,
        socket.userId,
        data.userId,
        data.userId,
        socket.userId
        );
        
        callback(rows);
    });

    socket.on('message delivered',async (data)=>{
        await db.run(
            `UPDATE private_messages
            SET status='delivered'
            WHERE messageId=?`,
            data.messageId
        );

        const senderUser =onlineUsers.get(data.senderId);
        if(senderUser){
            io.to(senderUser.socketId).emit('message delivered',{
                messageId: data.messageId
            });
        }

        });
    socket.on('messages seen',async (data)=>{
        await db.run(
            `UPDATE private_messages
            SET status='seen'
            WHERE senderId=?
            AND receiverId=?`,
            data.userId,
            socket.userId
        );

        const otherUser =onlineUsers.get(data.userId);

        if(otherUser){

            io.to(otherUser.socketId)
            .emit('messages seen',{
                seenBy: socket.userId
            });
        }

});

    socket.on(
        'generate summary',
        async(data,callback)=>{

            const rows = await db.all(
            `
            SELECT content
            FROM private_messages
            WHERE
            (senderId=? AND receiverId=?)
            OR
            (senderId=? AND receiverId=?)
            ORDER BY id
            `,
            socket.userId,
            data.userId,
            data.userId,
            socket.userId
            );

            const summary = `
                        Total Messages: ${rows.length}

                        First Message:
                        ${rows[0]?.content}

                        Last Message:
                        ${rows[rows.length - 1]?.content}

                        Conversation Duration:
                        ${rows.length} exchanges
                        `;

            callback(summary);

        }
    );

   
    
    socket.on('disconnect',()=>{
        onlineUsers.delete(socket.userId);
        io.emit('online users',[...onlineUsers.values()]);
        console.log("A user disconnected");
        socket.broadcast.emit('system message',`${socket.username} left the chat`);
    })
        if(!socket.recovered){
            try {
                await db.each('SELECT username,id, content ,timestamp FROM messages WHERE id > ?',
                    [socket.handshake.auth.serverOffset || 0],
                    (_err, row) => {
                    socket.emit('chat message',{
                        username:row.username,
                        message:row.content,
                        id:row.id,
                        timestamp:row.timestamp
                    });
                    }
                )
            }
            catch(e){
                console.error(e);
            } 
        }
    
})


}

main();