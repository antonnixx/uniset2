/*
 * Copyright (c) 2023 Pavel Vainerman.
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
// -----------------------------------------------------------------------------
#ifndef OPCUAClient_H_
#define OPCUAClient_H_
// -----------------------------------------------------------------------------
#include <string>
#include <vector>
#include <variant>
#include <open62541/client_config_default.h>
#include "Exceptions.h"
//--------------------------------------------------------------------------
namespace uniset
{
    // -----------------------------------------------------------------------------
    /*! Интерфейс для работы с OPC UA */
    class OPCUAClient
    {
        public:
            OPCUAClient();
            virtual ~OPCUAClient();

            bool connect( const std::string& addr );
            bool connect( const std::string& addr, const std::string& user, const std::string& pass );

            // supported types (other types are converted to these if possible)
            enum class VarType
            {
                Int32 = 0,
                Float = 1
            };
            static VarType str2vtype( std::string_view s );

            struct ResultVar
            {
                ResultVar() {}
                std::variant<int32_t, float> value = { 0 };
                UA_StatusCode status;
                VarType type = { VarType::Int32 }; // by default

                // get as int32_t (cast to int32_t if possible)
                int32_t get();

                template<class VType>
                VType as()
                {
                    try
                    {
                        return std::get<VType>(value);
                    }
                    catch(const std::bad_variant_access&) {}

                    return {};
                }
            };

            using ErrorCode = int;

            ErrorCode read( std::vector<UA_ReadValueId>& attrs, std::vector<ResultVar>& result );
            ErrorCode write32( std::vector<UA_WriteValue>& values );
            ErrorCode write32( const std::string& attr, int32_t value );
            ErrorCode set( const std::string& attr, bool set );
            ErrorCode write( const UA_WriteValue& val );
            static UA_WriteValue makeWriteValue32( const std::string& name, int32_t val );
            static UA_ReadValueId makeReadValue32( const std::string& name );

        protected:
            UA_Client* client = { nullptr };
            UA_Variant* val = { nullptr };
            UA_SessionState ss;

        private:
    };
    // --------------------------------------------------------------------------
} // end of namespace uniset
// -----------------------------------------------------------------------------
#endif // OPCUAClient_H_
// -----------------------------------------------------------------------------
