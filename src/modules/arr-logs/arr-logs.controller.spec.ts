import { Test, TestingModule } from '@nestjs/testing';
import { ArrLogsController } from './arr-logs.controller';
import { PrismaService } from '@/prisma/prisma.service';

describe('ArrLogsController', () => {
  let controller: ArrLogsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ArrLogsController],
    }).compile();

    controller = module.get<ArrLogsController>(ArrLogsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
