import { getArg } from "../utils/args.js";
import { adminList } from "./listMemory.js";

const vendor = getArg("vendor");
if (!vendor) {
  console.error("Missing --vendor");
  process.exit(1);
}

const simulateDays = Number(getArg("simulateDays", "0"));

adminList(vendor, simulateDays);
