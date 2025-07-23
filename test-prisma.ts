import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  const logs = await prisma.arrLogManual.findMany();
  console.log(logs);
}

main();
