#include "hdf5.h"

#include <iostream>
#include <string>

static bool TryOpen(const std::string &filename) {
	std::cerr << "Direct H5Fopen check: " << filename << std::endl;

	hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
	if (fapl >= 0) {
		H5Pset_fclose_degree(fapl, H5F_CLOSE_WEAK);
		H5Pset_file_locking(fapl, 0, 0);
	}

	hid_t file = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, fapl >= 0 ? fapl : H5P_DEFAULT);
	if (fapl >= 0) {
		H5Pclose(fapl);
	}

	if (file < 0) {
		std::cerr << "Direct H5Fopen failed: " << filename << std::endl;
		return false;
	}

	H5Fclose(file);
	std::cerr << "Direct H5Fopen succeeded: " << filename << std::endl;
	return true;
}

int main(int argc, char **argv) {
	if (argc < 2) {
		std::cerr << "usage: " << argv[0] << " <hdf5-file> [<hdf5-file> ...]" << std::endl;
		return 2;
	}

	bool ok = true;
	for (int i = 1; i < argc; i++) {
		ok = TryOpen(argv[i]) && ok;
	}
	return ok ? 0 : 1;
}
