import { Test, TestingModule } from '@nestjs/testing';
import { ArrStationsController } from './arr-stations.controller';

describe('ArrStationsController', () => {
  let controller: ArrStationsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ArrStationsController],
    }).compile();

    controller = module.get<ArrStationsController>(ArrStationsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
