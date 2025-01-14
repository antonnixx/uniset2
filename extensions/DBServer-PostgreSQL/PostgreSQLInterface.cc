/*
 * Copyright (c) 2015 Pavel Vainerman.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, version 2.1.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Lesser Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
// --------------------------------------------------------------------------
#include <sstream>
#include <cstdio>
#include "UniSetTypes.h"
#include "unisetstd.h"
#include "PostgreSQLInterface.h"
// --------------------------------------------------------------------------
using namespace std;
using namespace uniset;
using namespace pqxx;
// --------------------------------------------------------------------------

PostgreSQLInterface::PostgreSQLInterface():
    lastQ(""),
    lastE(""),
    last_inserted_id(0)
{
    //db = make_shared<pqxx::connection>();
}

PostgreSQLInterface::~PostgreSQLInterface()
{
    try
    {
        close();
    }
    catch( ... ) // пропускаем все необработанные исключения, если требуется обработать нужно вызывать close() до деструктора
    {
        cerr << "PostgresSQLInterface::~PostgresSQLInterface(): an error occured while closing connection!" << endl;
    }
}

// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::ping() const
{
    if( !db )
        return false;

    try
    {
        // pqxx doesn't work with unique_ptr (
        nontransaction n{*(db.get())};
        n.exec("select 1;");
        return true;
    }
    catch( const std::exception& e )
    {
        //      lastE = string(e.what());
    }

    return false;

    //  return db && db->is_open();
}
// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::reconnect(const string& host, const string& user, const string& pswd, const string& dbname, unsigned int port )
{
    if( db )
        close();

    return nconnect(host, user, pswd, dbname, port);
}
// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::nconnect(const string& host, const string& user, const string& pswd, const string& dbname, unsigned int port )
{
    if( db )
        return true;

    ostringstream conninfo;
    conninfo << "dbname=" << dbname
             << " host=" << host
             << " user=" << user
             << " password=" << pswd
             << " port=" << port;

    try
    {
        db = unisetstd::make_unique<pqxx::connection>( std::move(conninfo.str()) );
        return db->is_open();
    }
    catch( const std::exception& e )
    {
        cerr << e.what() << std::endl;
    }

    return false;
}
// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::close()
{
    if( db )
    {
        db->close();
        db.reset();
    }

    return true;
}
// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::copy( const std::string& tblname, const  std::initializer_list<std::string_view>& cols,
                                const PostgreSQLInterface::Data& data )
{
    if( !db )
    {
        lastE = "no connection";
        return false;
    }

    try
    {
        pqxx::work tx{ *(db.get()) };
        auto t{pqxx::stream_to::table(tx, {tblname}, cols)};

        for( const auto& d : data )
            t << d;

        t.complete();
        tx.commit();
        return true;
    }
    catch( const std::exception& e )
    {
        //cerr << e.what() << std::endl;
        lastE = string(e.what());
    }

    return false;
}
// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::insert( const string& q )
{
    if( !db )
    {
        lastE = "no connection";
        return false;
    }

    try
    {
        // pqxx doesn't work with unique_ptr
        work w( *(db.get()) );
        lastQ = q;
        w.exec(q);
        w.commit();
        return true;
    }
    catch( const std::exception& e )
    {
        //cerr << e.what() << std::endl;
        lastE = string(e.what());
    }

    return false;
}
// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::insertAndSaveRowid( const string& q )
{
    if( !db )
    {
        lastE = "no connection";
        return false;
    }

    std::string qplus = q + " RETURNING id";

    try
    {
        // pqxx doesn't work with unique_ptr
        work w( *(db.get()) );
        lastQ = q;
        pqxx::result res = w.exec(qplus);
        w.commit();
        save_inserted_id(res);
        return true;
    }
    catch( const std::exception& e )
    {
        //cerr << e.what() << std::endl;
        lastE = string(e.what());
    }

    return false;
}
// -----------------------------------------------------------------------------------------
DBResult PostgreSQLInterface::query( const string& q )
{
    if( !db )
        return DBResult();

    try
    {
        // pqxx doesn't work with unique_ptr
        nontransaction n{*(db.get())};
        lastQ = q;
        /* Execute SQL query */
        result res( n.exec(q) );
        return makeResult(res);
    }
    catch( const std::exception& e )
    {
        lastE = string(e.what());
    }

    return DBResult();
}
// -----------------------------------------------------------------------------------------
void PostgreSQLInterface::cancel_query()
{
    if( !db )
        return;

    db->cancel_query();
}
// -----------------------------------------------------------------------------------------
const string PostgreSQLInterface::error()
{
    return lastE;
}
// -----------------------------------------------------------------------------------------
const string PostgreSQLInterface::lastQuery()
{
    return lastQ;
}
// -----------------------------------------------------------------------------------------
double PostgreSQLInterface::insert_id()
{
    return last_inserted_id;
}
// -----------------------------------------------------------------------------------------
void PostgreSQLInterface::save_inserted_id( const pqxx::result& res )
{
    if( res.size() > 0 && res[0].size() > 0 )
        last_inserted_id = res[0][0].as<int>();
}
// -----------------------------------------------------------------------------------------
bool PostgreSQLInterface::isConnection() const
{
    return db && ping();
    //  return (db && db->is_open())
}
// -----------------------------------------------------------------------------------------
DBResult PostgreSQLInterface::makeResult( const pqxx::result& res )
{
    DBResult result;

    for( const auto& c : res )
    {
        DBResult::COL col;

        for( const auto& i : c )
        {
            if( i.is_null() )
                col.push_back("");
            else
            {
                result.setColName(i.num(), i.name());
                col.push_back( i.as<string>() );
            }
        }

        result.row().push_back( std::move(col) );
    }

    return result;
}
// -----------------------------------------------------------------------------------------
extern "C" std::shared_ptr<DBInterface> create_postgresqlinterface()
{
    return std::shared_ptr<DBInterface>(new PostgreSQLInterface(), DBInterfaceDeleter());
}
// -----------------------------------------------------------------------------------------
