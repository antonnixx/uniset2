#include <sstream>
#include "ObjectsActivator.h"
#include "Extensions.h"
#include "UNetExchange.h"
// -----------------------------------------------------------------------------
using namespace std;
using namespace UniSetTypes;
using namespace UniSetExtensions;
// -----------------------------------------------------------------------------
int main( int argc, char** argv )
{
	try
	{
		if( argc>1 && (!strcmp(argv[1],"--help") || !strcmp(argv[1],"-h")) )
		{
			cout << "--smemory-id objectName  - SharedMemory objectID. Default: read from <SharedMemory>" << endl;
			cout << "--confile filename       - configuration file. Default: configure.xml" << endl;
			cout << "--unet-logfile filename    - logfilename. Default: udpexchange.log" << endl;
			cout << endl;
			UNetExchange::help_print(argc,argv);
			return 0;
		}

		string confile=UniSetTypes::getArgParam("--confile",argc,argv,"configure.xml");
		conf = new Configuration( argc, argv, confile );

		string logfilename(conf->getArgParam("--unet-logfile"));
		if( logfilename.empty() )
			logfilename = "udpexchange.log";

		conf->initDebug(dlog,"dlog");

		std::ostringstream logname;
		string dir(conf->getLogDir());
		logname << dir << logfilename;
		unideb.logFile( logname.str() );
		dlog.logFile( logname.str() );

		ObjectId shmID = DefaultObjectId;
		string sID = conf->getArgParam("--smemory-id");
		if( !sID.empty() )
			shmID = conf->getControllerID(sID);
		else
			shmID = getSharedMemoryID();

		if( shmID == DefaultObjectId )
		{
			cerr << sID << "? SharedMemoryID not found in " << conf->getControllersSection() << " section" << endl;
			return 1;
		}

		UNetExchange* unet = UNetExchange::init_unetexchange(argc,argv,shmID);
		if( !unet )
		{
			dlog[Debug::CRIT] << "(unetexchange): init failed.." << endl;
			return 1;
		}

		ObjectsActivator act;
		act.addObject(static_cast<class UniSetObject*>(unet));

		SystemMessage sm(SystemMessage::StartUp);
		act.broadcast( sm.transport_msg() );

		unideb(Debug::ANY) << "\n\n\n";
		unideb[Debug::ANY] << "(main): -------------- UDPRecevier START -------------------------\n\n";
		dlog(Debug::ANY) << "\n\n\n";
		dlog[Debug::ANY] << "(main): -------------- UDPReceiver START -------------------------\n\n";

		act.run(false);
		return 0;
	}
	catch( Exception& ex )
	{
		dlog[Debug::CRIT] << "(unetexchange): " << ex << std::endl;
	}
	catch( ost::SockException& e )
	{
		ostringstream s;
		s << e.getString() << ": " << e.getSystemErrorString();
		dlog[Debug::CRIT] << s.str() << endl;
	}
	catch(...)
	{
		dlog[Debug::CRIT] << "(unetexchange): catch ..." << std::endl;
	}

	return 1;
}