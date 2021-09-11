import sqlite from "better-sqlite3";
import { useID } from "@dothq/id";

export class Database {
    private db: sqlite.Database & { serialize: () => Buffer };

    private readonly name: string;
    private readonly writer: (serialized: Buffer) => any;
    private readonly loader: () => any;

    public init = false;

    public e(sql: string, ctx?: object) {
        const result = this.db.prepare(sql).run(ctx ? ctx : {});

        this.save();

        return result;
    }

    public get(query?: object) {
        const sqlWhere = query 
            ? (` WHERE `) + (Object.entries(query)
                .map(([key, value]) => `${key} = ?`)
                .join(" AND "))
            : ``;

        const q = this.db.prepare(`SELECT * FROM ${this.name}${sqlWhere}`).bind(
            ...(query ? Object.values(query) : [])
        );

        const result = q.all();

        if(!result.length) return undefined; 

        if(query && Object.keys(query).length) {
            if(result[0] && "data" in result[0]) {
                return this.formatJSON(result[0].data).parsed
            } else {
                return undefined;
            }
        } else {
            return result.map(r => {
                if(r && "data" in r) {
                    return this.formatJSON(r.data).parsed
                } else {
                    return undefined;
                }
            })
        }
    }

    public insert(data: any) {
        if(!Object.keys(data).length) throw new Error(`Cannot insert empty object!`);

        const q = this.db.prepare(`INSERT INTO ${this.name} VALUES (@id, @data)`);

        const id = data.id 
            ? data.id 
            : useID(4);

        if(this.get({ id })) throw new Error(`Object with id "${id}" already exists.`)

        const result = { 
            id, 
            data, 
            ...q.run({
                id, 
                data: this.formatJSON(data).stringified
            }) 
        };

        this.save();

        return result;
    }

    public delete(id: any) {
        const q = this.db
            .prepare(`DELETE FROM ${this.name} WHERE id = @id`)

        q.run({ id });

        this.save();

        if(this.get({ id })) throw new Error(`Unable to delete item with id "${id}".`)
        else return true;
    }

    public save() {
        if(!this.writer) return true;

        const raw = this.raw() || Buffer.from("");

        return this.writer(raw)
    }

    private formatJSON(data: any) {
        let parsed: any;
        let stringified: any;

        try {
            parsed = JSON.parse(data);
        } catch(e) {}

        try {
            stringified = JSON.stringify(data);
        } catch(e) {}

        return { parsed, stringified };
    }

    public raw() {
        const raw = this.db.serialize();
        
        return raw;
    }

    public async load() {
        if(this.init) return;

        let db: any;

        if(this.loader) {
            let data: any;

            try {
                data = await this.loader();
            } catch(e) { }

            try {
                if(data) {
                    db = sqlite(data) as any;
                }
            } catch(e) { }
        }

        if(!db) {
            db = sqlite(":memory:") as any;
        }

        this.db = db;
        this.init = true;

        this.e(`CREATE TABLE IF NOT EXISTS ${this.name} (id TEXT, data TEXT)`);
    }

    public constructor({
        name,
        loader,
        writer
    }: {
        name: string,
        loader?: () => any,
        writer?: (serialized: Buffer) => any
    }) {
        this.name = name;
        this.writer = writer;
        this.loader = loader;
    }
}