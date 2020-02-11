#pragma once

#include <config/exports.hpp>
#include <core/specification.hpp>
#include <util/algorithm_name_conversion.hpp>
#include <string>
#include <stdexcept>

namespace hype
{
	namespace core{
		class HYPE_EXPORT Report
		{	public:
			static Report& instance();
			
			double getRelativeEstimationError(const std::string& algorithm_name, const DeviceSpecification& dev_spec) const throw(std::invalid_argument);
			
			private:
				Report();
				Report(const Report& report);
				Report& operator=(const Report& report);
		};

	};//end namespace core
}; //end namespace hype	

