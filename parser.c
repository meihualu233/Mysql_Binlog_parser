#include "common.h"

/*
* 定义的全局信息
*/
//全局文件句柄
FILE* fp;

//头结构体
struct BinlogEventHeader format_description_event_header;

//表结构map
map<string,string> mp; //表名 对应的字段 名
map<long,string> table_mp; //表的id 对应的表
map<long,vector<int> > type_mp; //表每个字段对应的类型
////////

int main()
{
    fp = fopen("mysql-bin.000003", "rb");
    if(fp == NULL)
    {
        printf("File not exist!\n");
        return -1;
    }
    int magic_number;
    //判断magic num 是否正常
    fread(&magic_number, 4, 1, fp);

    printf("%d - %s\n", magic_number, (char*)(&magic_number));
    if(magic_number != MAGIC_NUMBER)
    {
        printf("File is not BinLog!\n");
        fclose(fp);
        return -1;
    }

    fseek(fp, 0L, SEEK_END);		//定位文件指针到尾部
	long file_len = ftell(fp);		//获得文件长度
	fseek(fp, 4L, SEEK_SET);		//已经读取了4字节的头

    //读 并 分析
    while(!feof(fp))
    {
        fread(&format_description_event_header, HEADER_LEN, 1, fp);
        // 打印解析的头信息
        //print_Header();

        // 这个Event的结束位置
        int next_position = format_description_event_header.next_position;

        /* 解析内容 根据EVENT的TYPE*/
        switch (format_description_event_header.type_code)
        {
        case FORMAT_DESCRIPTION_EVENT:
            //printf("Type : FORMAT_DESCRIPTION_EVENT\n");
            process_format();
            break;
        case QUERY_EVENT:
            //printf("Type : QUERY_EVENT\n");
            process_query();
            break;
        case TABLE_MAP_EVENT:
            //printf("Type : TABLE_MAP_EVENT\n");
            process_table_map();
            break;
        case WRITE_ROWS_EVENT:
            //printf("Type : WRITE_ROWS_EVENT\n");
            process_write();
            break;
        case UPDATE_ROWS_EVENT:
            //printf("Type : UPDATE_ROWS_EVENT\n");
            process_update();
            break;
        case DELETE_ROWS_EVENT:
            //printf("Type : DELETE_ROWS_EVENT\n");
            process_delete();
            break;
        //代表commit
        case XID_EVENT:
            //printf("Type : XID_EVENT\n");
            process_xid();
            break;
        }
        //移动指针到下一个位置
        printf("!!!!!!! %d\n",next_position);
        if(next_position == file_len) break;
        fseek(fp,next_position,SEEK_SET);
    }
    return 0;
}


void print_Header()
{
    printf("BinlogEventHeader\n{\n");
    printf("    timestamp: %d\n", format_description_event_header.timestamp);
    printf("    type_code: %d\n", format_description_event_header.type_code);
    printf("    server_id: %d\n", format_description_event_header.server_id);
    printf("    event_length: %d\n", format_description_event_header.event_length);
    printf("    next_position: %d\n", format_description_event_header.next_position);
    printf("    flags[]: %d\n}\n", format_description_event_header.flags);
}

/*
* format_desc Event 解析
*/
void process_format()
{
    short binlog_version;
    fread(&binlog_version, 2, 1, fp);
    //printf("binlog_version: %d\n", binlog_version);

    char server_version[51];
    fread(server_version, 50, 1, fp);
    server_version[50] = '\0';
    //printf("server_version: %s\n", server_version);

    int create_timestamp;
    fread(&create_timestamp, 4, 1, fp);
    //printf("create_timestamp: %d\n", create_timestamp);

    char header_length;
    fread(&header_length, 1, 1, fp);
    //printf("header_length: %d\n", header_length);

    int type_count = format_description_event_header.event_length - 76;
    unsigned char post_header_length[type_count];
    fread(post_header_length, 1, type_count, fp);
    /*
    for(int i = 0; i < type_count; i++)
    {
        printf("  - type %d: %d\n", i + 1, post_header_length[i]);
    }
    */
}

/*
* 提取出表对应的字段
*/
void fuck(string database_name,string sql)
{
    //printf("%s\n %s",database_name,sql);
    string table_name="";
    table_name+=database_name+'.' ;
    /*先判断 是否是创建表*/
    int idx = sql.find("CREATE");
    if(idx == string::npos)
    {
        idx = sql.find("create");
        if(idx == string::npos)
        {
            return ;
        }
    }
    idx = sql.find("TABLE");
    if(idx==string::npos)
    {
        idx = sql.find("table");
        if(idx==string::npos) return ;
    }
    //cout<<sql<<endl;
    idx+=5;
    for(;sql[idx]==' ' || sql[idx]=='\n' || sql[idx]=='\r';idx++) ;
    while(sql[idx]!=' ')
    {
        table_name+=sql[idx++];
    }
    /*以 数据库名.表名 的形式记录该表*/
    cout<<"Debug:"<<table_name<<endl;

    /*解析表结构*/
    string table_content="(";
    int len = sql.size();
    int end_pos;
    for(int i=len-1;i>=0;i--)
    {
        if(sql[i]==')') { end_pos = i; break; }
    }
    //1.找到第一个(
    idx = sql.find("(");
    string tmp ="`";
    while(true)
    {
        if((sql[idx]>='a' && sql[idx]<='z')||
            (sql[idx]>='A' && sql[idx]<='Z'))
        break;
        idx++;
    }
    for(;sql[idx]!=' ';idx++)
        tmp+=sql[idx];
    table_content+=tmp+"`";

    //2.找到,
    while(true)
    {
        idx = sql.find(",",idx);
        if(idx==string::npos || idx > end_pos)
        {
            //结束了
            table_content+=")";
            break;
        }
        else
        {
            tmp="";
            //去掉空格
            while(true)
            {
                if((sql[idx]>='a' && sql[idx]<='z')||
                    (sql[idx]>='A' && sql[idx]<='Z'))
                break;
                idx++;
            }
            for(;sql[idx]!=' ';idx++)
                tmp+=sql[idx];
            //排除掉主键等的可能
            if(tmp=="PRIMARY" || tmp=="primary" || tmp=="UNIQUE" || tmp=="unique")
            {
                //结束了
                table_content+=')';
                break;
            }
            else
            {
                table_content+=",`";
                table_content+=tmp+"`" ;
            }
        }
        //cout<<table_content<<endl;
        //system("pause");
    }
    cout<<"Debug:" <<table_content<<endl;
    mp[table_name] = table_content;
}

/*
* QUERY_EVENT 解析
*/
void process_query()
{
    /*fixed part*/
    int thread_id;
    fread(&thread_id,4,1,fp);
    //printf("thread_id: %d\n",thread_id);

    int process_time;
    fread(&process_time,4,1,fp);
    //printf("process_time: %d\n",process_time);

    /*数据库 长度*/
    char len;
    fread(&len,1,1,fp);
    //printf("database len: %d\n",len);

    short errorcode;
    fread(&errorcode,2,1,fp);
    //printf("error code: %d\n",errorcode);

    short status_vars_block_len ;
    fread(&status_vars_block_len,2,1,fp);
    //printf("status_vars_block_len : %d\n",status_vars_block_len);

    //跨越一些没用的信息
    char buffer[1024];
    fread(buffer,status_vars_block_len,1,fp);

    char database_name[512];
    fread(database_name,len+1,1,fp);
    database_name[len]='\0';
    printf("use '%s';\n",database_name);

    /*variable parts*/
    char sql[1024];
    memset(sql,0,sizeof(sql));
    //sql len = event_length - status_vars_block_len - 固定头长度 - 数据库的名字 - fixed length - 校验和 4字节
    int sql_len = format_description_event_header.event_length - status_vars_block_len - 19 -13 -len-1 -4;
    fread(sql,sql_len,1,fp);
    sql[sql_len] = '\0';
    //为了提取Insert/upsate/delete的字段 需要解析出对应表的各个字段名

    fuck((string)database_name,(string)sql);

    printf("%s\n",sql);
    printf("***************************\n");
    //system("pause");
}

/*
* TABLE_MAP 解析
*/
void process_table_map()
{
    /*fixed part*/
    long table_id;
    fread(&table_id,6,1,fp);
    //printf("table_id : %d\n",table_id);

    //跨越一些没用的信息
    char buffer[3];
    fread(buffer,2,1,fp);

    /*variable part*/
    //数据库名长度 and数据库名
    char database_name_len ;
    fread(&database_name_len,1,1,fp);
    printf("database len: %d\n",database_name_len);

    char database_name[512];
    fread(&database_name,database_name_len+1,1,fp);
    database_name[database_name_len]='\0';
    printf("database name: %s\n",database_name);

    //表名长度 and 表名
    char table_name_len ;
    fread(&table_name_len,1,1,fp);
    printf("database len: %d\n",table_name_len);

    char table_name[512];
    fread(&table_name,table_name_len+1,1,fp);
    table_name[table_name_len]='\0';
    printf("table name: %s\n",table_name);

    string tmp ="";
    tmp+= database_name;
    tmp+=".";
    tmp+=table_name;
    table_mp[table_id]= tmp;

    cout<<"Debug: "<<mp[tmp]<<endl;
    /*字段 type*/
    char cnt_type;
    fread(&cnt_type,1,1,fp);
    cout<<"Debug :"<<(int)cnt_type<<endl;


    vector<int> vt;
    for(int i=0;i<(int)cnt_type;i++)
    {
        char tmp_type;
        fread(&tmp_type,1,1,fp);
        cout<<(int)tmp_type<<endl;
        switch(tmp_type)
        {
        case MYSQL_TYPE_TINY:
            vt.push_back(C_CHAR);
            break;
        case MYSQL_TYPE_SHORT:
            vt.push_back(C_SHORT);
            break;
        case MYSQL_TYPE_LONG:
            vt.push_back(C_INT);
            break;
        case MYSQL_TYPE_FLOAT:
            vt.push_back(C_FLOAT);
            break;
        case MYSQL_TYPE_DOUBLE:
            vt.push_back(C_DOUBLE);
            break;
        case MYSQL_TYPE_LONGLONG:
            vt.push_back(C_LONG_LONG);
            break;
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_DATETIME:
            vt.push_back(C_TIME);
            break;
        default :
            vt.push_back(C_CHARS);
            break;
        }
    }
    for(int i=0;i<vt.size();i++)
        cout<<"Debug :"<<vt[i]<<" ";
    cout<<endl;
    printf("*******************\n");
    //将表id 和字段类型做好映射
    type_mp[table_id] = vt;
    //system("pause");
}

/*
* WRITE_ROWS_EVENT/DELETE_ROWS_EVENT/UPDATE_ROWS_EVENT 解析
* 三种sql的日志解析一样 不同的就在于前面的 insert \delete\update
* 因为insert 语句较为复杂，使用固定格式显示 sql
* insert into + 表名 + (字段) + values + ( 内容 )
*/
void process_write()
{
    /*fixed part*/
    long table_id;
    fread(&table_id,6,1,fp);
    //printf("table_id : %d\n",table_id);

    //跨越一些没用的信息
    char buf[5];
    fread(buf,4,1,fp);
    /*variable part*/
    //字段数量
    char cnt_type;
    fread(&cnt_type,1,1,fp);
    printf("Debug : %d\n",(int)cnt_type);

    //跳过FF 源码描述为 m_cols;	/* Bitmap denoting columns available */
    char t;
    fread(&t,1,1,fp);

    cout<<"INSERT INTO "<<table_mp[table_id]<<" "<<mp[table_mp[table_id]]
        <<" VALUES (";

    //解析每个字段是否为空
    //cnt_type个字段 需要  cnt_type+7 /8 字节 =n
    // s[n] 存储每个字节 小端存储 要从右往左找
    //0 表示不是null | 1=null
    int tmp;
    int n = (cnt_type + 7) / 8;
    char s[n+1]; s[n]='\0';
    fread(&s,n,1,fp);
    int m=n-1; //从这个字节开始往前找
    //总共cnt_type个字段
    vector<int> vt = type_mp[table_id];

    for(int i=0;i<cnt_type;i++)
    {
        //vector<int> vt = type_mp[table_id];
        tmp = ( s[ m-(i/8) ]>> (i%8) )&1 ;
        if(tmp == 1)
        {
            //null
            //cout<<"NULL"<<endl;
            cout<<"'NULL'";
            if(i!=cnt_type-1) cout<<",";
        }
        else
        {
            //不是null ，判断类型

            //cout<<vt[i]<<endl;
            switch(vt[i])
            {
            case C_CHAR:
                char type;
                fread(&type,1,1,fp);
                cout<<"'"<<type<<"'";
                break;
            case C_SHORT:
                short type1;
                fread(&type1,2,1,fp);
                cout<<"'"<<type1<<"'";
                break;
            case C_INT:
                int type2;
                fread(&type2,4,1,fp);
                cout<<"'"<<type2<<"'";
                break;
            case C_FLOAT:
                float type3;
                fread(&type3,4,1,fp);
                cout<<"'"<<type3<<"'";
                break;
            case C_DOUBLE:
                double type4;
                fread(&type4,8,1,fp);
                cout<<"'"<<type4<<"'";
                break;
            case C_LONG_LONG:
                long long type5;
                fread(&type5,8,1,fp);
                cout<<"'"<<type5<<"'";
                break;
            default :
                char type6[1024];
                short len ;
                fread(&len,2,1,fp);
                cout<<"len: "<<len<<endl;
                fread(&type6,len,1,fp);
                type6[len]='\0';
                cout<<"'"<<type6<<"'";
                break;
            }
            if(i!=cnt_type-1) cout<<",";
        }
    }
    cout<<")"<<endl;

    //system("pause");
}
/*
* update
* 遇到一个麻烦，因为二进制文件是按照顺序存入的，一个文件写满了后就会写另外一个文件
* 如果单独分析一个文件，将没有上文信息 此时update语句不能正确解析
* 这里使用 update + 表名 + where + （更改前） + set + (更改后) 进行显示
*/
//TO DO: 时间戳格式的为 99 A0 65 50 41 B5 不知道怎么解析 -> 2018-07-18 21:01:01
void process_update()
{
    /*fixed part*/
    long table_id;
    fread(&table_id,6,1,fp);
    printf("table_id : %d\n",table_id);

    //跨越一些没用的信息
    char buf[5];
    fread(&buf,4,1,fp);

    /*variable part*/
    //字段数量
    char cnt_type;
    fread(&cnt_type,1,1,fp);
    printf("Debug : %d\n",(int)cnt_type);

    //跳过FF 源码描述为 m_cols;	/* Bitmap denoting columns available */
    char t[3];
    fread(&t,2,1,fp);

    cout<<"UPDATE "<<table_mp[table_id]<<" WHERE ";

    //解析每个字段是否为空
    //cnt_type个字段 需要  cnt_type+7 /8 字节 =n
    // s[n] 存储每个字节 小端存储 要从右往左找
    //0 表示不是null | 1=null
    int tmp;
    int n = (cnt_type + 7) / 8;

    //需要解析两次，更改前 和 更改后的数据
    for(int j=0;j<2;j++)
    {
        char s[n+1]; s[n]='\0';
        fread(&s,n,1,fp);
        int m=n-1; //从这个字节开始往前找
        //总共cnt_type个字段
        vector<int> vt = type_mp[table_id];
        //for(int i=0;i<vt.size();i++)
         //   cout<<vt[i]<<endl;
        //system("pause");
        cout<<"(";

        for(int i=0;i<cnt_type;i++)
        {
            //vector<int> vt = type_mp[table_id];
            tmp = ( s[ m-(i/8) ]>> (i%8) )&1 ;
            if(tmp == 1)
            {
                //null
                //cout<<"NULL"<<endl;
                cout<<"'NULL'";
                if(i!=cnt_type-1) cout<<",";
            }
            else
            {
                //不是null ，判断类型

                //cout<<vt[i]<<endl;
                switch(vt[i])
                {
                case C_CHAR:
                    char type;
                    fread(&type,1,1,fp);
                    cout<<"'"<<type<<"'";
                    break;
                case C_SHORT:
                    short type1;
                    fread(&type1,2,1,fp);
                    cout<<"'"<<type1<<"'";
                    break;
                case C_INT:
                    int type2;
                    fread(&type2,4,1,fp);
                    cout<<"'"<<type2<<"'";
                    break;
                case C_FLOAT:
                    float type3;
                    fread(&type3,4,1,fp);
                    cout<<"'"<<type3<<"'";
                    break;
                case C_DOUBLE:
                    double type4;
                    fread(&type4,8,1,fp);
                    cout<<"'"<<type4<<"'";
                    break;
                case C_LONG_LONG:
                    long long type5;
                    fread(&type5,8,1,fp);
                    cout<<"'"<<type5<<"'";
                    break;
                case C_TIME:
                    char type_time[6];
                    fread(&type_time,5,1,fp);
                    cout<<"'TIME'";
                    break;
                default :
                    char type6[1024];
                    short len ;
                    fread(&len,2,1,fp);
                    //cout<<"len: "<<len<<endl;
                    fread(&type6,len,1,fp);
                    type6[len]='\0';
                    cout<<"'"<<type6<<"'";
                    break;
                }
                if(i!=cnt_type-1) cout<<",";
            }
        }
        cout<<")"<<endl;

        //system("pause");
    }
}


/*
* delete
* 遇到的问题和上面的一样
* delete from + 表名 + (字段) + set + ( 内容 )
*/
//TO DO: 时间戳格式的为 99 A0 65 50 41 B5 不知道怎么解析 -> 2018-07-18 21:01:01
void process_delete()
{
    /*fixed part*/
    long table_id;
    fread(&table_id,6,1,fp);
    //printf("table_id : %d\n",table_id);

    //跨越一些没用的信息
    char buf[5];
    fread(buf,4,1,fp);
    /*variable part*/
    //字段数量
    char cnt_type;
    fread(&cnt_type,1,1,fp);
    printf("Debug : %d\n",(int)cnt_type);

    //跳过FF 源码描述为 m_cols;	/* Bitmap denoting columns available */
    char t;
    fread(&t,1,1,fp);

    cout<<"DELETE FROM "<<table_mp[table_id]<<" WHERE "
        <<mp[table_mp[table_id]]<<" SET (";

    //解析每个字段是否为空
    //cnt_type个字段 需要  cnt_type+7 /8 字节 =n
    // s[n] 存储每个字节 小端存储 要从右往左找
    //0 表示不是null | 1=null
    int tmp;
    int n = (cnt_type + 7) / 8;
    char s[n+1]; s[n]='\0';
    fread(&s,n,1,fp);
    int m=n-1; //从这个字节开始往前找
    //总共cnt_type个字段
    vector<int> vt = type_mp[table_id];

    for(int i=0;i<cnt_type;i++)
    {
        //vector<int> vt = type_mp[table_id];
        tmp = ( s[ m-(i/8) ]>> (i%8) )&1 ;
        if(tmp == 1)
        {
            //null
            //cout<<"NULL"<<endl;
            cout<<"'NULL'";
            if(i!=cnt_type-1) cout<<",";
        }
        else
        {
            //不是null ，判断类型

            //cout<<vt[i]<<endl;
            switch(vt[i])
            {
            case C_CHAR:
                char type;
                fread(&type,1,1,fp);
                cout<<"'"<<type<<"'";
                break;
            case C_SHORT:
                short type1;
                fread(&type1,2,1,fp);
                cout<<"'"<<type1<<"'";
                break;
            case C_INT:
                int type2;
                fread(&type2,4,1,fp);
                cout<<"'"<<type2<<"'";
                break;
            case C_FLOAT:
                float type3;
                fread(&type3,4,1,fp);
                cout<<"'"<<type3<<"'";
                break;
            case C_DOUBLE:
                double type4;
                fread(&type4,8,1,fp);
                cout<<"'"<<type4<<"'";
                break;
            case C_LONG_LONG:
                long long type5;
                fread(&type5,8,1,fp);
                cout<<"'"<<type5<<"'";
                break;
            default :
                char type6[1024];
                short len ;
                fread(&len,2,1,fp);
                //cout<<"len: "<<len<<endl;
                fread(&type6,len,1,fp);
                type6[len]='\0';
                cout<<"'"<<type6<<"'";
                break;
            }
            if(i!=cnt_type-1) cout<<",";
        }
    }
    cout<<")"<<endl;

    //system("pause");
}

/*
* XID_EVENT 解析
*/
void process_xid()
{
    printf("COMMIT;\n");
}
