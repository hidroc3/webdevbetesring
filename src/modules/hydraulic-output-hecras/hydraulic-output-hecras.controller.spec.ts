import { Test, TestingModule } from '@nestjs/testing';
import { HydraulicOutputHecrasController } from './hydraulic-output-hecras.controller';

describe('HydraulicOutputHecrasController', () => {
  let controller: HydraulicOutputHecrasController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [HydraulicOutputHecrasController],
    }).compile();

    controller = module.get<HydraulicOutputHecrasController>(HydraulicOutputHecrasController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
