import { Test, TestingModule } from '@nestjs/testing';
import { ArrLogManualsController } from './arr-log-manuals.controller';

describe('ArrLogManualsController', () => {
  let controller: ArrLogManualsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ArrLogManualsController],
    }).compile();

    controller = module.get<ArrLogManualsController>(ArrLogManualsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
